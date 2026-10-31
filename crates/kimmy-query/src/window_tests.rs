//! `$setWindowFields` (ADR-264): what it answers, and what it refuses.
//!
//! The expected values are worked out by hand from the reference's documented behaviour.

use bson::{Bson, Document, doc};
use kimmy_core::Error;

use crate::aggregate::{self, Limits, Stage};
use crate::window;

fn run(stage: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&[stage])?;
    aggregate::apply(&stages[0], input, &Limits::default())
}

fn refused(stage: Document) -> String {
    aggregate::parse(&[stage]).expect_err("must be refused").to_string()
}

/// A stage that parses but whose run is refused.
fn refused_run(stage: Document, input: Vec<Document>) -> String {
    run(stage, input).expect_err("must be refused at run time").to_string()
}

fn set(spec: Document) -> Document {
    doc! {"$setWindowFields": spec}
}

/// One output field of every document, in output order.
fn col(out: &[Document], field: &str) -> Vec<Bson> {
    out.iter().map(|d| d.get(field).cloned().unwrap_or(Bson::Null)).collect()
}

fn ids(out: &[Document]) -> Vec<i32> {
    out.iter().map(|d| d.get_i32("_id").unwrap()).collect()
}

fn i64s(values: &[i64]) -> Vec<Bson> {
    values.iter().map(|v| Bson::Int64(*v)).collect()
}

fn f64s(values: &[f64]) -> Vec<Bson> {
    values.iter().map(|v| Bson::Double(*v)).collect()
}

fn near(actual: &[Bson], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len(), "{actual:?}");
    for (a, e) in actual.iter().zip(expected) {
        match a {
            Bson::Double(a) => assert!((a - e).abs() < 1e-9, "{actual:?} vs {expected:?}"),
            other => panic!("{other:?} is not a double in {actual:?}"),
        }
    }
}

/// Two states, four days in California and two in Washington.
fn sales() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "state": "CA", "day": 1, "qty": 10},
        doc! {"_id": 2, "state": "CA", "day": 2, "qty": 20},
        doc! {"_id": 3, "state": "CA", "day": 3, "qty": 30},
        doc! {"_id": 4, "state": "CA", "day": 4, "qty": 40},
        doc! {"_id": 5, "state": "WA", "day": 1, "qty": 5},
        doc! {"_id": 6, "state": "WA", "day": 2, "qty": 15},
    ]
}

/// Documents with only a `qty`, numbered from 1.
fn series(values: &[i32]) -> Vec<Document> {
    values.iter().enumerate().map(|(i, q)| doc! {"_id": i as i32 + 1, "qty": *q}).collect()
}

fn d(text: &str) -> bson::DateTime {
    bson::DateTime::parse_rfc3339_str(text).unwrap()
}

// ---------------------------------------------------------------------------
// Documents windows
// ---------------------------------------------------------------------------

#[test]
fn a_cumulative_sum_runs_from_the_start_of_each_partition() {
    let out = run(
        set(doc! {"partitionBy": "$state", "sortBy": {"day": 1},
        "output": {"total": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}}}}),
        sales(),
    )
    .unwrap();
    assert_eq!(ids(&out), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(col(&out, "total"), i64s(&[10, 30, 60, 100, 5, 20]));
}

#[test]
fn a_moving_average_over_the_neighbours_clips_at_the_partition_edges() {
    let out = run(
        set(doc! {"partitionBy": "$state", "sortBy": {"day": 1},
        "output": {"avg": {"$avg": "$qty", "window": {"documents": [-1, 1]}}}}),
        sales(),
    )
    .unwrap();
    // CA: (10+20)/2, (10+20+30)/3, (20+30+40)/3, (30+40)/2; WA: (5+15)/2 twice.
    near(&col(&out, "avg"), &[15.0, 20.0, 30.0, 35.0, 10.0, 10.0]);
}

#[test]
fn with_no_window_the_whole_partition_is_the_window() {
    let out =
        run(set(doc! {"partitionBy": "$state", "output": {"total": {"$sum": "$qty"}}}), sales())
            .unwrap();
    assert_eq!(col(&out, "total"), i64s(&[100, 100, 100, 100, 20, 20]));
}

#[test]
fn no_partition_and_no_sort_is_one_partition_in_arrival_order() {
    let out = run(
        set(doc! {"output": {"n": {"$count": {}}, "all": {"$push": "$qty"}}}),
        series(&[3, 1, 2]),
    )
    .unwrap();
    assert_eq!(ids(&out), vec![1, 2, 3]);
    assert_eq!(col(&out, "n"), vec![Bson::Int32(3); 3]);
    assert_eq!(
        col(&out, "all")[0],
        Bson::Array(vec![Bson::Int32(3), Bson::Int32(1), Bson::Int32(2)])
    );
}

#[test]
fn documents_leave_partition_by_partition_in_first_seen_order_and_sorted_within() {
    let input = vec![
        doc! {"_id": 1, "state": "WA", "day": 2},
        doc! {"_id": 2, "state": "CA", "day": 9},
        doc! {"_id": 3, "state": "WA", "day": 1},
        doc! {"_id": 4, "state": "CA", "day": 3},
    ];
    let out = run(
        set(doc! {"partitionBy": "$state", "sortBy": {"day": 1}, "output": {"n": {"$documentNumber": {}}}}),
        input.clone(),
    )
    .unwrap();
    assert_eq!(ids(&out), vec![3, 1, 4, 2]);
    assert_eq!(col(&out, "n"), i64s(&[1, 2, 1, 2]));
    // Descending.
    let out = run(
        set(doc! {"partitionBy": "$state", "sortBy": {"day": -1}, "output": {"n": {"$documentNumber": {}}}}),
        input,
    )
    .unwrap();
    assert_eq!(ids(&out), vec![1, 3, 2, 4]);
}

#[test]
fn partitions_are_keyed_as_group_keys_are_so_one_and_one_point_zero_are_together() {
    let input = vec![
        doc! {"_id": 1, "k": 1, "qty": 1},
        doc! {"_id": 2, "k": 1.0, "qty": 2},
        doc! {"_id": 3, "k": 2, "qty": 4},
    ];
    let out =
        run(set(doc! {"partitionBy": "$k", "output": {"t": {"$sum": "$qty"}}}), input).unwrap();
    assert_eq!(col(&out, "t"), i64s(&[3, 3, 4]));
}

#[test]
fn a_partition_expression_may_compute_the_key() {
    let out = run(
        set(doc! {"partitionBy": {"$mod": ["$qty", 2]}, "sortBy": {"qty": 1},
        "output": {"t": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}}}}),
        series(&[1, 2, 3, 4, 5]),
    )
    .unwrap();
    // Odd (1, 3, 5) first as the first seen, then the even.
    assert_eq!(ids(&out), vec![1, 3, 5, 2, 4]);
    assert_eq!(col(&out, "t"), i64s(&[1, 4, 9, 2, 6]));
}

#[test]
fn a_window_beyond_the_partition_is_empty_and_answers_what_an_empty_group_does() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "sum": {"$sum": "$qty", "window": {"documents": [1, 2]}},
            "avg": {"$avg": "$qty", "window": {"documents": [1, 2]}},
            "min": {"$min": "$qty", "window": {"documents": [1, 2]}},
            "max": {"$max": "$qty", "window": {"documents": [1, 2]}},
            "push": {"$push": "$qty", "window": {"documents": [1, 2]}},
            "set": {"$addToSet": "$qty", "window": {"documents": [1, 2]}},
            "first": {"$first": "$qty", "window": {"documents": [1, 2]}},
            "last": {"$last": "$qty", "window": {"documents": [1, 2]}},
            "n": {"$count": {}, "window": {"documents": [1, 2]}},
            "sd": {"$stdDevPop": "$qty", "window": {"documents": [1, 2]}},
        }}),
        series(&[1, 2, 3]),
    )
    .unwrap();
    // The last document has nothing after it.
    let last = &out[2];
    assert_eq!(last.get("sum"), Some(&Bson::Int64(0)));
    for field in ["avg", "min", "max", "first", "last", "sd"] {
        assert_eq!(last.get(field), Some(&Bson::Null), "{field}");
    }
    assert_eq!(last.get("push"), Some(&Bson::Array(vec![])));
    assert_eq!(last.get("set"), Some(&Bson::Array(vec![])));
    assert_eq!(last.get("n"), Some(&Bson::Int32(0)));
    // The first document sees the two after it.
    assert_eq!(out[0].get("sum"), Some(&Bson::Int64(5)));
    assert_eq!(out[0].get("push"), Some(&Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])));
    assert_eq!(out[1].get("first"), Some(&Bson::Int32(3)));
    assert_eq!(out[0].get("last"), Some(&Bson::Int32(3)));
}

#[test]
fn a_window_wholly_behind_the_current_document_works() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
        "prev2": {"$sum": "$qty", "window": {"documents": [-3, -1]}}}}),
        series(&[1, 2, 4, 8, 16]),
    )
    .unwrap();
    assert_eq!(col(&out, "prev2"), i64s(&[0, 1, 3, 7, 14]));
}

#[test]
fn min_and_max_slide_over_a_monotone_deque() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "lo": {"$min": "$qty", "window": {"documents": [-1, 1]}},
            "hi": {"$max": "$qty", "window": {"documents": [-1, 1]}},
        }}),
        series(&[5, 3, 8, 1, 9]),
    )
    .unwrap();
    assert_eq!(
        col(&out, "lo"),
        vec![Bson::Int32(3), Bson::Int32(3), Bson::Int32(1), Bson::Int32(1), Bson::Int32(1)]
    );
    assert_eq!(
        col(&out, "hi"),
        vec![Bson::Int32(5), Bson::Int32(8), Bson::Int32(8), Bson::Int32(9), Bson::Int32(9)]
    );
}

#[test]
fn min_and_max_skip_null_and_missing_and_keep_the_first_of_equal_values() {
    let input = vec![
        doc! {"_id": 1, "v": 2},
        doc! {"_id": 2, "v": Bson::Null},
        doc! {"_id": 3},
        doc! {"_id": 4, "v": 2.0},
    ];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "lo": {"$min": "$v", "window": {"documents": [-3, 0]}},
        }}),
        input,
    )
    .unwrap();
    // `2` (an int) arrived before `2.0` and stays the answer.
    assert_eq!(
        col(&out, "lo"),
        vec![Bson::Int32(2), Bson::Int32(2), Bson::Int32(2), Bson::Int32(2)]
    );
}

#[test]
fn first_and_last_read_the_edges_of_the_window() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "f": {"$first": "$qty", "window": {"documents": [-1, 1]}},
            "l": {"$last": "$qty", "window": {"documents": [-1, 1]}},
        }}),
        series(&[10, 20, 30]),
    )
    .unwrap();
    assert_eq!(col(&out, "f"), vec![Bson::Int32(10), Bson::Int32(10), Bson::Int32(20)]);
    assert_eq!(col(&out, "l"), vec![Bson::Int32(20), Bson::Int32(30), Bson::Int32(30)]);
}

#[test]
fn push_and_add_to_set_are_rebuilt_as_the_window_slides() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "p": {"$push": "$qty", "window": {"documents": [-1, 0]}},
            "s": {"$addToSet": "$qty", "window": {"documents": [-2, 0]}},
        }}),
        series(&[1, 1, 2, 2]),
    )
    .unwrap();
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    assert_eq!(col(&out, "p"), vec![ints(&[1]), ints(&[1, 1]), ints(&[1, 2]), ints(&[2, 2])]);
    assert_eq!(col(&out, "s"), vec![ints(&[1]), ints(&[1]), ints(&[1, 2]), ints(&[1, 2])]);
}

#[test]
fn a_standard_deviation_slides_and_a_one_value_sample_is_null() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "pop": {"$stdDevPop": "$qty", "window": {"documents": [-1, 0]}},
            "samp": {"$stdDevSamp": "$qty", "window": {"documents": [-1, 0]}},
            "all": {"$stdDevPop": "$qty"},
        }}),
        series(&[2, 4, 4, 4, 5, 5, 7, 9]),
    )
    .unwrap();
    near(&col(&out, "pop"), &[0.0, 1.0, 0.0, 0.0, 0.5, 0.0, 1.0, 1.0]);
    // The sample deviation of two values a and b is |a - b| / sqrt 2.
    let samp = col(&out, "samp");
    assert_eq!(samp[0], Bson::Null);
    assert!(matches!(samp[1], Bson::Double(x) if (x - 2f64.sqrt()).abs() < 1e-9), "{samp:?}");
    // The textbook set has a population deviation of exactly 2.
    near(&col(&out, "all")[..1], &[2.0]);
}

#[test]
fn count_slides_and_is_an_int() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"n": {"$count": {}, "window": {"documents": [-1, 0]}}}}),
        series(&[1, 2, 3]),
    )
    .unwrap();
    assert_eq!(col(&out, "n"), vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(2)]);
}

#[test]
fn the_ranked_and_n_accumulators_work_over_windows() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "top": {"$top": {"sortBy": {"qty": -1}, "output": "$_id"}, "window": {"documents": [-1, 0]}},
            "bottom": {"$bottomN": {"n": 2, "sortBy": {"qty": -1}, "output": "$qty"}, "window": {"documents": ["unbounded", "current"]}},
            "first2": {"$firstN": {"n": 2, "input": "$qty"}, "window": {"documents": [-1, 1]}},
            "max2": {"$maxN": {"n": 2, "input": "$qty"}},
        }}),
        series(&[10, 40, 20, 30]),
    )
    .unwrap();
    // The _id of the larger qty of the document and the one before it.
    assert_eq!(
        col(&out, "top"),
        vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(2), Bson::Int32(4)]
    );
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    // Cumulatively, the two smallest, the smallest last in a descending sortBy.
    assert_eq!(
        col(&out, "bottom"),
        vec![ints(&[10]), ints(&[40, 10]), ints(&[20, 10]), ints(&[20, 10])]
    );
    assert_eq!(
        col(&out, "first2"),
        vec![ints(&[10, 40]), ints(&[10, 40]), ints(&[40, 20]), ints(&[20, 30])]
    );
    assert_eq!(col(&out, "max2")[0], ints(&[40, 30]));
}

#[test]
fn a_sum_with_a_decimal_in_the_window_is_decimal_and_exact_again_once_it_leaves() {
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    let input =
        vec![doc! {"_id": 1, "v": dec("1.5")}, doc! {"_id": 2, "v": 2}, doc! {"_id": 3, "v": 3}];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"s": {"$sum": "$v", "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "s"), vec![dec("1.5"), dec("3.5"), Bson::Int64(5)]);
}

#[test]
fn a_sum_past_the_range_of_a_long_widens_as_a_group_does_and_comes_back() {
    let input = vec![
        doc! {"_id": 1, "v": i64::MAX},
        doc! {"_id": 2, "v": 1_i64},
        doc! {"_id": 3, "v": 2_i64},
    ];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"s": {"$sum": "$v", "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    assert_eq!(
        col(&out, "s"),
        vec![Bson::Int64(i64::MAX), Bson::Double(9223372036854775808.0), Bson::Int64(3)]
    );
}

#[test]
fn a_sum_mixing_ints_and_doubles_is_a_double_and_skips_other_types() {
    let input = vec![
        doc! {"_id": 1, "v": 1},
        doc! {"_id": 2, "v": 0.5},
        doc! {"_id": 3, "v": "x"},
        doc! {"_id": 4, "v": 4},
    ];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$v", "window": {"documents": [-1, 0]}},
            "a": {"$avg": "$v", "window": {"documents": [-1, 0]}},
        }}),
        input,
    )
    .unwrap();
    assert_eq!(
        col(&out, "s"),
        vec![Bson::Int64(1), Bson::Double(1.5), Bson::Double(0.5), Bson::Int64(4)]
    );
    assert_eq!(col(&out, "a"), f64s(&[1.0, 0.75, 0.5, 4.0]));
}

#[test]
fn a_sliding_sum_agrees_with_adding_up_each_window() {
    // A fixed pseudo-random series and several windows, against the plain sum of each window.
    let mut x: u64 = 12345;
    let mut vals = Vec::new();
    for _ in 0..200 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        vals.push(((x >> 33) % 2001) as i64 - 1000);
    }
    let input: Vec<Document> =
        vals.iter().enumerate().map(|(i, v)| doc! {"_id": i as i32, "v": *v}).collect();
    for (lo, hi) in [(-2i64, 1i64), (-7, -3), (2, 9), (0, 0), (-50, 50)] {
        let out = run(
            set(doc! {"sortBy": {"_id": 1}, "output": {
                "s": {"$sum": "$v", "window": {"documents": [lo, hi]}},
                "m": {"$min": "$v", "window": {"documents": [lo, hi]}},
                "a": {"$avg": "$v", "window": {"documents": [lo, hi]}},
            }}),
            input.clone(),
        )
        .unwrap();
        for (i, o) in out.iter().enumerate() {
            let s = ((i as i64 + lo).max(0) as usize).min(vals.len());
            let e = ((i as i64 + hi + 1).max(0) as usize).min(vals.len()).max(s);
            let window = &vals[s..e];
            assert_eq!(o.get("s"), Some(&Bson::Int64(window.iter().sum())), "[{lo},{hi}] at {i}");
            assert_eq!(
                o.get("m"),
                Some(&window.iter().min().map_or(Bson::Null, |m| Bson::Int64(*m))),
                "[{lo},{hi}] at {i}"
            );
            match (o.get("a"), window.len()) {
                (Some(Bson::Null), 0) => {}
                (Some(Bson::Double(a)), n) if n > 0 => {
                    let want = window.iter().sum::<i64>() as f64 / n as f64;
                    assert!((a - want).abs() < 1e-9, "[{lo},{hi}] at {i}");
                }
                other => panic!("{other:?}"),
            }
        }
    }
}

#[test]
fn a_sliding_window_over_a_large_partition_is_not_quadratic() {
    // 60,000 documents and a window of 1,001: adding up every window would be sixty million
    // evaluations; sliding is a hundred and twenty thousand.
    let n = 60_000;
    let input: Vec<Document> = (0..n).map(|i| doc! {"_id": i, "v": 1}).collect();
    let started = std::time::Instant::now();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$v", "window": {"documents": [-500, 500]}},
            "c": {"$count": {}, "window": {"documents": [-500, 500]}},
            "lo": {"$min": "$v", "window": {"documents": [-500, 500]}},
        }}),
        input,
    )
    .unwrap();
    assert!(started.elapsed().as_secs() < 20, "took {:?}", started.elapsed());
    assert_eq!(out[0].get("s"), Some(&Bson::Int64(501)));
    assert_eq!(out[30_000].get("s"), Some(&Bson::Int64(1001)));
    assert_eq!(out[n as usize - 1].get("c"), Some(&Bson::Int32(501)));
}

#[test]
fn outputs_read_the_original_documents_never_each_other() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "qty": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}},
            "again": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}},
        }}),
        series(&[1, 2, 3]),
    )
    .unwrap();
    assert_eq!(col(&out, "qty"), i64s(&[1, 3, 6]));
    assert_eq!(col(&out, "again"), i64s(&[1, 3, 6]));
}

#[test]
fn an_output_may_be_a_dotted_path() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"stats.running": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}}}}),
        vec![doc! {"_id": 1, "qty": 4, "stats": {"keep": true}}, doc! {"_id": 2, "qty": 5}],
    )
    .unwrap();
    assert_eq!(out[0].get_document("stats").unwrap(), &doc! {"keep": true, "running": 4i64});
    assert_eq!(out[1].get_document("stats").unwrap(), &doc! {"running": 9i64});
}

#[test]
fn no_documents_in_none_out_and_an_empty_output_changes_nothing() {
    assert!(run(set(doc! {"output": {"n": {"$count": {}}}}), vec![]).unwrap().is_empty());
    let out = run(set(doc! {"output": {}}), series(&[1, 2])).unwrap();
    assert_eq!(out, series(&[1, 2]));
}

// ---------------------------------------------------------------------------
// Range windows
// ---------------------------------------------------------------------------

#[test]
fn a_range_window_counts_distance_in_the_sort_value_not_positions() {
    let input = vec![
        doc! {"_id": 1, "day": 1, "qty": 1},
        doc! {"_id": 2, "day": 2, "qty": 2},
        doc! {"_id": 3, "day": 4, "qty": 4},
        doc! {"_id": 4, "day": 7, "qty": 8},
    ];
    let out = run(
        set(doc! {"sortBy": {"day": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [-2, 0]}}}}),
        input,
    )
    .unwrap();
    // Day 1: {1}; day 2: {1, 2}; day 4: {2, 4}; day 7: {7}.
    assert_eq!(col(&out, "s"), i64s(&[1, 3, 6, 8]));
}

#[test]
fn a_range_window_includes_every_document_tied_with_the_current_one() {
    let input = vec![
        doc! {"_id": 1, "day": 1, "qty": 1},
        doc! {"_id": 2, "day": 1, "qty": 2},
        doc! {"_id": 3, "day": 2, "qty": 4},
    ];
    let out = run(
        set(doc! {"sortBy": {"day": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": ["current", "current"]}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "s"), i64s(&[3, 3, 4]));
}

#[test]
fn a_range_window_unbounded_on_one_side() {
    let out = run(
        set(doc! {"sortBy": {"qty": 1}, "output": {
            "up": {"$sum": "$qty", "window": {"range": ["unbounded", 1]}},
            "down": {"$sum": "$qty", "window": {"range": [-1, "unbounded"]}},
        }}),
        series(&[1, 2, 4, 5]),
    )
    .unwrap();
    // qty 1: values up to 2 = 3; qty 2: up to 3 = 3; qty 4: up to 5 = 12; qty 5: 12.
    assert_eq!(col(&out, "up"), i64s(&[3, 3, 12, 12]));
    // qty 1: from 0 = 12; qty 2: from 1 = 12; qty 4: from 3 = 9; qty 5: from 4 = 9.
    assert_eq!(col(&out, "down"), i64s(&[12, 12, 9, 9]));
}

#[test]
fn a_range_window_follows_the_sort_direction_when_descending() {
    let out = run(
        set(doc! {"sortBy": {"day": -1}, "output": {"s": {"$sum": "$day", "window": {"range": [-3, 0]}}}}),
        vec![doc! {"day": 1}, doc! {"day": 2}, doc! {"day": 4}, doc! {"day": 7}],
    )
    .unwrap();
    // Sorted 7, 4, 2, 1; the window reaches back (up the number line) by 3: 7 -> {7}, 4 -> {7, 4},
    // 2 -> {4, 2}, 1 -> {4, 2, 1}.
    assert_eq!(col(&out, "s"), i64s(&[7, 11, 6, 7]));
}

#[test]
fn a_range_window_over_dates_steps_by_the_unit() {
    let input = vec![
        doc! {"_id": 1, "t": d("2023-01-01T00:00:00Z"), "qty": 1},
        doc! {"_id": 2, "t": d("2023-01-01T01:00:00Z"), "qty": 2},
        doc! {"_id": 3, "t": d("2023-01-01T03:00:00Z"), "qty": 4},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [-1, 0], "unit": "hour"}}}}),
        input.clone(),
    )
    .unwrap();
    assert_eq!(col(&out, "s"), i64s(&[1, 3, 4]));
    // Fractions of a fixed-length unit.
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [-0.5, 0], "unit": "hour"}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "s"), i64s(&[1, 2, 4]));
}

#[test]
fn a_range_window_in_months_is_a_calendar_step_clamped_to_the_month_end() {
    let input = vec![
        doc! {"_id": 1, "t": d("2023-01-31T00:00:00Z"), "qty": 1},
        doc! {"_id": 2, "t": d("2023-02-28T00:00:00Z"), "qty": 2},
        doc! {"_id": 3, "t": d("2023-03-31T00:00:00Z"), "qty": 4},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [-1, 0], "unit": "month"}}}}),
        input,
    )
    .unwrap();
    // One month before 28 February is 28 January, which takes in 31 January; one month before
    // 31 March is 28 February (the month's end), which takes in 28 February.
    assert_eq!(col(&out, "s"), i64s(&[1, 3, 6]));
}

#[test]
fn every_date_unit_is_accepted() {
    for unit in
        ["year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"]
    {
        run(
            set(doc! {"sortBy": {"t": 1}, "output": {"s": {"$count": {}, "window": {"range": [-1, 1], "unit": unit}}}}),
            vec![doc! {"t": d("2023-01-31T00:00:00Z")}],
        )
        .unwrap_or_else(|e| panic!("{unit}: {e}"));
    }
}

#[test]
fn a_range_window_needs_a_sort_value_of_one_kind_in_every_document() {
    let w = |spec: Document| {
        set(doc! {"sortBy": {"v": 1}, "output": {"s": {"$sum": 1, "window": spec}}})
    };
    let missing = refused_run(w(doc! {"range": [-1, 0]}), vec![doc! {"v": 1}, doc! {"x": 1}]);
    assert!(missing.contains("number or a date") && missing.contains("missing"), "{missing}");
    let text = refused_run(w(doc! {"range": [-1, 0]}), vec![doc! {"v": "a"}]);
    assert!(text.contains("number or a date"), "{text}");
    let both = refused_run(
        w(doc! {"range": [-1, 0], "unit": "day"}),
        vec![doc! {"v": 1}, doc! {"v": d("2023-01-01T00:00:00Z")}],
    );
    assert!(both.contains("all numbers or all dates"), "{both}");
    let numeric_unit = refused_run(w(doc! {"range": [-1, 0], "unit": "day"}), vec![doc! {"v": 1}]);
    assert!(numeric_unit.contains("date sortBy"), "{numeric_unit}");
    let date_no_unit =
        refused_run(w(doc! {"range": [-1, 0]}), vec![doc! {"v": d("2023-01-01T00:00:00Z")}]);
    assert!(date_no_unit.contains("needs a `unit`"), "{date_no_unit}");
    let nan = refused_run(w(doc! {"range": [-1, 0]}), vec![doc! {"v": f64::NAN}]);
    assert!(nan.contains("number or a date"), "{nan}");
}

// ---------------------------------------------------------------------------
// Window bounds refusals
// ---------------------------------------------------------------------------

fn bounds(window: Document) -> String {
    refused(set(doc! {"sortBy": {"v": 1}, "output": {"s": {"$sum": 1, "window": window}}}))
}

#[test]
fn window_bounds_are_validated() {
    assert!(bounds(doc! {"documents": [1, -1]}).contains("lower bound 1 must not be above"));
    assert!(bounds(doc! {"documents": [0, -1]}).contains("must not be above"));
    assert!(bounds(doc! {"range": [2, 1]}).contains("must not be above"));
    assert!(bounds(doc! {"documents": [1.5, 2]}).contains("whole number"));
    assert!(bounds(doc! {"documents": ["now", 2]}).contains("\"unbounded\", \"current\""));
    assert!(bounds(doc! {"documents": [1]}).contains("two bounds"));
    assert!(bounds(doc! {"documents": [1, 2, 3]}).contains("two bounds"));
    assert!(bounds(doc! {"documents": "unbounded"}).contains("two bounds"));
    assert!(bounds(doc! {"range": ["x", 1]}).contains("\"unbounded\", \"current\""));
    assert!(bounds(doc! {"range": [Bson::Null, 1]}).contains("number"));
    assert!(bounds(doc! {}).contains("needs `documents` or `range`"));
    assert!(bounds(doc! {"documents": [0, 1], "range": [0, 1]}).contains("one of"));
    assert!(bounds(doc! {"documents": [0, 1], "unit": "day"}).contains("goes with `range`"));
    assert!(bounds(doc! {"range": [0, 1], "unit": "fortnight"}).contains("`unit` must be one of"));
    assert!(bounds(doc! {"range": [0, 1], "unit": 5}).contains("`unit` must be one of"));
    assert!(bounds(doc! {"range": [0.5, 1], "unit": "month"}).contains("whole numbers"));
    assert!(bounds(doc! {"documents": [0, 1], "extra": 1}).contains("does not take `extra`"));
    let not_doc = refused(set(doc! {"output": {"s": {"$sum": 1, "window": [0, 1]}}}));
    assert!(not_doc.contains("`window` must be a document"), "{not_doc}");
}

#[test]
fn a_range_window_needs_exactly_one_sort_field() {
    let none = refused(set(doc! {"output": {"s": {"$sum": 1, "window": {"range": [0, 1]}}}}));
    assert!(none.contains("exactly one field"), "{none}");
    let two = refused(set(
        doc! {"sortBy": {"a": 1, "b": 1}, "output": {"s": {"$sum": 1, "window": {"range": [0, 1]}}}},
    ));
    assert!(two.contains("exactly one field"), "{two}");
    // A documents window over two sort fields is fine.
    run(
        set(doc! {"sortBy": {"a": 1, "b": 1}, "output": {"s": {"$sum": 1, "window": {"documents": [0, 1]}}}}),
        vec![doc! {"a": 1, "b": 2}],
    )
    .unwrap();
}

#[test]
fn bounds_of_the_whole_partition_are_equal_to_no_window() {
    let a = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"p": {"$push": "$qty", "window": {"documents": ["unbounded", "unbounded"]}}}}),
        series(&[1, 2]),
    )
    .unwrap();
    let b =
        run(set(doc! {"sortBy": {"_id": 1}, "output": {"p": {"$push": "$qty"}}}), series(&[1, 2]))
            .unwrap();
    assert_eq!(a, b);
}

// ---------------------------------------------------------------------------
// Rank, document number, shift
// ---------------------------------------------------------------------------

#[test]
fn rank_dense_rank_and_document_number_over_ties() {
    let input = vec![
        doc! {"_id": 1, "score": 90},
        doc! {"_id": 2, "score": 80},
        doc! {"_id": 3, "score": 80},
        doc! {"_id": 4, "score": 70},
    ];
    let out = run(
        set(doc! {"sortBy": {"score": -1}, "output": {
        "rank": {"$rank": {}}, "dense": {"$denseRank": {}}, "n": {"$documentNumber": {}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "rank"), i64s(&[1, 2, 2, 4]));
    assert_eq!(col(&out, "dense"), i64s(&[1, 2, 2, 3]));
    assert_eq!(col(&out, "n"), i64s(&[1, 2, 3, 4]));
}

#[test]
fn rank_ties_are_decided_on_every_sort_key_and_restart_in_each_partition() {
    let input = vec![
        doc! {"_id": 1, "g": "a", "x": 1, "y": 1},
        doc! {"_id": 2, "g": "a", "x": 1, "y": 2},
        doc! {"_id": 3, "g": "a", "x": 1, "y": 2},
        doc! {"_id": 4, "g": "b", "x": 1, "y": 1},
    ];
    let out = run(
        set(doc! {"partitionBy": "$g", "sortBy": {"x": 1, "y": 1}, "output": {"rank": {"$rank": {}}, "dense": {"$denseRank": {}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "rank"), i64s(&[1, 2, 2, 1]));
    assert_eq!(col(&out, "dense"), i64s(&[1, 2, 2, 1]));
}

#[test]
fn a_missing_sort_value_ranks_with_null() {
    let out = run(
        set(doc! {"sortBy": {"s": 1}, "output": {"rank": {"$rank": {}}}}),
        vec![doc! {"_id": 1, "s": 1}, doc! {"_id": 2}, doc! {"_id": 3, "s": Bson::Null}],
    )
    .unwrap();
    // Null and missing sort first and tie.
    assert_eq!(ids(&out), vec![2, 3, 1]);
    assert_eq!(col(&out, "rank"), i64s(&[1, 1, 3]));
}

#[test]
fn rank_operators_refuse_a_window_a_missing_sort_and_arguments() {
    for op in ["$rank", "$denseRank", "$documentNumber"] {
        let w = refused(set(
            doc! {"sortBy": {"a": 1}, "output": {"r": {op: {}, "window": {"documents": [0, 1]}}}},
        ));
        assert!(w.contains("does not take a `window`"), "{op}: {w}");
        let s = refused(set(doc! {"output": {"r": {op: {}}}}));
        assert!(s.contains("requires a `sortBy`"), "{op}: {s}");
        let a = refused(set(doc! {"sortBy": {"a": 1}, "output": {"r": {op: {"x": 1}}}}));
        assert!(a.contains("takes no arguments"), "{op}: {a}");
        let n = refused(set(doc! {"sortBy": {"a": 1}, "output": {"r": {op: 1}}}));
        assert!(n.contains("takes no arguments"), "{op}: {n}");
    }
}

#[test]
fn shift_reads_a_document_ahead_or_behind_and_defaults_beyond_the_ends() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "next": {"$shift": {"output": "$qty", "by": 1}},
            "prev": {"$shift": {"output": "$qty", "by": -1, "default": 0}},
            "same": {"$shift": {"output": "$qty", "by": 0}},
            "far": {"$shift": {"output": "$qty", "by": 5, "default": "none"}},
        }}),
        series(&[10, 20, 30]),
    )
    .unwrap();
    let ints = |v: &[i32]| v.iter().map(|x| Bson::Int32(*x)).collect::<Vec<_>>();
    assert_eq!(col(&out, "next"), vec![Bson::Int32(20), Bson::Int32(30), Bson::Null]);
    assert_eq!(col(&out, "prev"), ints(&[0, 10, 20]));
    assert_eq!(col(&out, "same"), ints(&[10, 20, 30]));
    assert_eq!(col(&out, "far"), vec![Bson::String("none".into()); 3]);
}

#[test]
fn shift_stays_inside_its_partition() {
    let out = run(
        set(doc! {"partitionBy": "$state", "sortBy": {"day": 1}, "output": {"next": {"$shift": {"output": "$qty", "by": 1, "default": -1}}}}),
        sales(),
    )
    .unwrap();
    let ints = |v: &[i32]| v.iter().map(|x| Bson::Int32(*x)).collect::<Vec<_>>();
    assert_eq!(col(&out, "next"), ints(&[20, 30, 40, -1, 15, -1]));
}

#[test]
fn shift_refusals() {
    let r = |arg: Bson| refused(set(doc! {"sortBy": {"a": 1}, "output": {"s": {"$shift": arg}}}));
    assert!(r(Bson::Document(doc! {"output": "$a"})).contains("requires `by`"));
    assert!(r(Bson::Document(doc! {"by": 1})).contains("requires `output`"));
    assert!(r(Bson::Document(doc! {"output": "$a", "by": 1.5})).contains("whole number"));
    assert!(r(Bson::Document(doc! {"output": "$a", "by": "x"})).contains("whole number"));
    assert!(
        r(Bson::Document(doc! {"output": "$a", "by": 1, "default": "$b"})).contains("constant")
    );
    assert!(
        r(Bson::Document(doc! {"output": "$a", "by": 1, "other": 1}))
            .contains("unknown argument: other")
    );
    assert!(r(Bson::Int32(1)).contains("requires an object"));
    let w = refused(set(
        doc! {"sortBy": {"a": 1}, "output": {"s": {"$shift": {"output": "$a", "by": 1}, "window": {"documents": [0, 1]}}}},
    ));
    assert!(w.contains("does not take a `window`"), "{w}");
    let s = refused(set(doc! {"output": {"s": {"$shift": {"output": "$a", "by": 1}}}}));
    assert!(s.contains("requires a `sortBy`"), "{s}");
}

// ---------------------------------------------------------------------------
// Derivative, integral, moving average
// ---------------------------------------------------------------------------

fn xy() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "x": 1, "y": 10},
        doc! {"_id": 2, "x": 2, "y": 20},
        doc! {"_id": 3, "x": 4, "y": 50},
    ]
}

#[test]
fn a_derivative_is_the_slope_between_the_ends_of_the_window() {
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"d": {"$derivative": {"input": "$y"}, "window": {"documents": [-1, 0]}}}}),
        xy(),
    )
    .unwrap();
    // One document is no slope; then (20-10)/(2-1) and (50-20)/(4-2).
    assert_eq!(col(&out, "d"), vec![Bson::Null, Bson::Double(10.0), Bson::Double(15.0)]);
    // Over the whole partition: (50-10)/(4-1).
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"d": {"$derivative": {"input": "$y"}, "window": {"documents": ["unbounded", "unbounded"]}}}}),
        xy(),
    )
    .unwrap();
    near(&col(&out, "d"), &[40.0 / 3.0; 3]);
}

#[test]
fn a_derivative_over_dates_is_per_the_unit() {
    let input = vec![
        doc! {"t": d("2023-01-01T00:00:00Z"), "y": 10},
        doc! {"t": d("2023-01-01T02:00:00Z"), "y": 20},
    ];
    let per = |unit: &str| {
        run(
            set(doc! {"sortBy": {"t": 1}, "output": {"d": {"$derivative": {"input": "$y", "unit": unit}, "window": {"documents": [-1, 0]}}}}),
            input.clone(),
        )
        .unwrap()
    };
    // 10 over two hours.
    assert_eq!(col(&per("hour"), "d")[1], Bson::Double(5.0));
    assert_eq!(col(&per("minute"), "d")[1], Bson::Double(5.0 / 60.0));
    assert_eq!(col(&per("millisecond"), "d")[1], Bson::Double(10.0 / 7_200_000.0));
    // With no unit, milliseconds.
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"d": {"$derivative": {"input": "$y"}, "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "d")[1], Bson::Double(10.0 / 7_200_000.0));
}

#[test]
fn a_derivative_over_equal_sort_values_is_null() {
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"d": {"$derivative": {"input": "$y"}, "window": {"documents": [-1, 0]}}}}),
        vec![doc! {"x": 1, "y": 1}, doc! {"x": 1, "y": 2}],
    )
    .unwrap();
    assert_eq!(col(&out, "d"), vec![Bson::Null, Bson::Null]);
}

#[test]
fn an_integral_is_the_trapezoid_sum_over_the_window() {
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"i": {"$integral": {"input": "$y"}, "window": {"documents": ["unbounded", "current"]}}}}),
        xy(),
    )
    .unwrap();
    // 0; (2-1)*(10+20)/2 = 15; 15 + (4-2)*(20+50)/2 = 85.
    assert_eq!(col(&out, "i"), f64s(&[0.0, 15.0, 85.0]));
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"i": {"$integral": {"input": "$y"}, "window": {"documents": [-1, 0]}}}}),
        xy(),
    )
    .unwrap();
    assert_eq!(col(&out, "i"), f64s(&[0.0, 15.0, 70.0]));
}

#[test]
fn an_integral_over_dates_is_in_the_unit() {
    let input = vec![
        doc! {"t": d("2023-01-01T00:00:00Z"), "y": 10},
        doc! {"t": d("2023-01-01T02:00:00Z"), "y": 20},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"i": {"$integral": {"input": "$y", "unit": "hour"}, "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    // 2 hours at a mean of 15.
    assert_eq!(col(&out, "i")[1], Bson::Double(30.0));
}

#[test]
fn derivative_and_integral_take_a_range_window() {
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"i": {"$integral": {"input": "$y"}, "window": {"range": [-2, 0]}}}}),
        xy(),
    )
    .unwrap();
    // x = 4 reaches back to x = 2: one trapezoid, (4-2)*(20+50)/2.
    assert_eq!(col(&out, "i"), f64s(&[0.0, 15.0, 70.0]));
}

#[test]
fn derivative_and_integral_refuse_what_they_cannot_read() {
    for op in ["$derivative", "$integral"] {
        let r = |spec: Document| refused(set(spec));
        let no_window = r(doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y"}}}});
        assert!(no_window.contains("requires a `window`"), "{op}: {no_window}");
        let no_sort =
            r(doc! {"output": {"d": {op: {"input": "$y"}, "window": {"documents": [-1, 0]}}}});
        assert!(no_sort.contains("exactly one field"), "{op}: {no_sort}");
        let two = r(
            doc! {"sortBy": {"x": 1, "z": 1}, "output": {"d": {op: {"input": "$y"}, "window": {"documents": [-1, 0]}}}},
        );
        assert!(two.contains("exactly one field"), "{op}: {two}");
        let unit = r(
            doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y", "unit": "month"}, "window": {"documents": [-1, 0]}}}},
        );
        assert!(unit.contains("`unit` must be one of week, day"), "{op}: {unit}");
        let no_input = r(
            doc! {"sortBy": {"x": 1}, "output": {"d": {op: {}, "window": {"documents": [-1, 0]}}}},
        );
        assert!(no_input.contains("requires `input`"), "{op}: {no_input}");
        let other = r(
            doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y", "n": 1}, "window": {"documents": [-1, 0]}}}},
        );
        assert!(other.contains("unknown argument: n"), "{op}: {other}");
        // A run: a unit on a numeric sort, a non-numeric input, a non-numeric sort value.
        let w = doc! {"documents": [-1, 0]};
        let numeric_unit = refused_run(
            set(
                doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y", "unit": "day"}, "window": w.clone()}}},
            ),
            xy(),
        );
        assert!(numeric_unit.contains("date sortBy"), "{op}: {numeric_unit}");
        let text = refused_run(
            set(
                doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y"}, "window": w.clone()}}},
            ),
            vec![doc! {"x": 1, "y": 1}, doc! {"x": 2, "y": "a"}],
        );
        assert!(text.contains("numeric input"), "{op}: {text}");
        let null = refused_run(
            set(
                doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y"}, "window": w.clone()}}},
            ),
            vec![doc! {"x": 1, "y": 1}, doc! {"x": 2}],
        );
        assert!(null.contains("null or a missing"), "{op}: {null}");
        let sort = refused_run(
            set(doc! {"sortBy": {"x": 1}, "output": {"d": {op: {"input": "$y"}, "window": w}}}),
            vec![doc! {"x": "a", "y": 1}],
        );
        assert!(sort.contains("number or a date"), "{op}: {sort}");
    }
}

#[test]
fn an_exponential_moving_average_by_n_or_by_alpha() {
    let by_n = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$qty", "N": 3}}}}),
        series(&[10, 20, 30]),
    )
    .unwrap();
    // N = 3 is alpha 0.5: 10; 0.5*20 + 0.5*10; 0.5*30 + 0.5*15.
    assert_eq!(col(&by_n, "e"), f64s(&[10.0, 15.0, 22.5]));
    let by_alpha = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$qty", "alpha": 0.25}}}}),
        series(&[8, 4]),
    )
    .unwrap();
    assert_eq!(col(&by_alpha, "e"), f64s(&[8.0, 7.0]));
    // N = 1 is alpha 1: the value itself.
    let one = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$qty", "N": 1}}}}),
        series(&[8, 4]),
    )
    .unwrap();
    assert_eq!(col(&one, "e"), f64s(&[8.0, 4.0]));
}

#[test]
fn a_moving_average_skips_a_non_numeric_value_and_restarts_in_each_partition() {
    let out = run(
        set(doc! {"partitionBy": "$g", "sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$v", "alpha": 0.5}}}}),
        vec![
            doc! {"_id": 1, "g": 1, "v": 10},
            doc! {"_id": 2, "g": 1},
            doc! {"_id": 3, "g": 1, "v": 30},
            doc! {"_id": 4, "g": 2, "v": 6},
        ],
    )
    .unwrap();
    assert_eq!(
        col(&out, "e"),
        vec![Bson::Double(10.0), Bson::Null, Bson::Double(20.0), Bson::Double(6.0)]
    );
}

#[test]
fn moving_average_refusals() {
    let r = |arg: Document| {
        refused(set(doc! {"sortBy": {"a": 1}, "output": {"e": {"$expMovingAvg": arg}}}))
    };
    assert!(r(doc! {"input": "$a"}).contains("`N` or `alpha`"));
    assert!(r(doc! {"input": "$a", "N": 2, "alpha": 0.5}).contains("exactly one of"));
    assert!(r(doc! {"input": "$a", "N": 0}).contains("at least 1"));
    assert!(r(doc! {"input": "$a", "N": 1.5}).contains("whole number"));
    assert!(r(doc! {"input": "$a", "alpha": 1.0}).contains("greater than 0 and less than 1"));
    assert!(r(doc! {"input": "$a", "alpha": 0.0}).contains("greater than 0 and less than 1"));
    assert!(r(doc! {"input": "$a", "alpha": "x"}).contains("must be a number"));
    assert!(r(doc! {"N": 2}).contains("requires `input`"));
    assert!(r(doc! {"input": "$a", "N": 2, "beta": 1}).contains("unknown argument: beta"));
    let w = refused(set(
        doc! {"sortBy": {"a": 1}, "output": {"e": {"$expMovingAvg": {"input": "$a", "N": 2}, "window": {"documents": [0, 1]}}}},
    ));
    assert!(w.contains("does not take a `window`"), "{w}");
    let s = refused(set(doc! {"output": {"e": {"$expMovingAvg": {"input": "$a", "N": 2}}}}));
    assert!(s.contains("requires a `sortBy`"), "{s}");
}

// ---------------------------------------------------------------------------
// $locf and $linearFill as window operators
// ---------------------------------------------------------------------------

#[test]
fn locf_carries_the_last_non_null_value_forward() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"f": {"$locf": "$v"}}}),
        vec![
            doc! {"_id": 1},
            doc! {"_id": 2, "v": 5},
            doc! {"_id": 3, "v": Bson::Null},
            doc! {"_id": 4},
            doc! {"_id": 5, "v": 9},
            doc! {"_id": 6, "v": 0},
        ],
    )
    .unwrap();
    assert_eq!(
        col(&out, "f"),
        vec![
            Bson::Null,
            Bson::Int32(5),
            Bson::Int32(5),
            Bson::Int32(5),
            Bson::Int32(9),
            Bson::Int32(0)
        ]
    );
}

#[test]
fn linear_fill_interpolates_on_the_sort_value() {
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"f": {"$linearFill": "$v"}}}),
        vec![
            doc! {"x": 0},
            doc! {"x": 1, "v": 10},
            doc! {"x": 2},
            doc! {"x": 4},
            doc! {"x": 5, "v": 40},
            doc! {"x": 6},
        ],
    )
    .unwrap();
    // Leading and trailing gaps stay null; between 10 at x=1 and 40 at x=5 the slope is 7.5.
    assert_eq!(
        col(&out, "f"),
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
fn linear_fill_over_dates_and_its_refusals() {
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"f": {"$linearFill": "$v"}}}),
        vec![
            doc! {"t": d("2023-01-01T00:00:00Z"), "v": 0},
            doc! {"t": d("2023-01-01T01:00:00Z")},
            doc! {"t": d("2023-01-01T04:00:00Z"), "v": 40},
        ],
    )
    .unwrap();
    assert_eq!(col(&out, "f"), vec![Bson::Int32(0), Bson::Double(10.0), Bson::Int32(40)]);
    let text = refused_run(
        set(doc! {"sortBy": {"x": 1}, "output": {"f": {"$linearFill": "$v"}}}),
        vec![doc! {"x": 1, "v": "a"}],
    );
    assert!(text.contains("needs numbers"), "{text}");
    let two =
        refused(set(doc! {"sortBy": {"x": 1, "y": 1}, "output": {"f": {"$linearFill": "$v"}}}));
    assert!(two.contains("exactly one field"), "{two}");
    for op in ["$locf", "$linearFill"] {
        let w = refused(set(
            doc! {"sortBy": {"x": 1}, "output": {"f": {op: "$v", "window": {"documents": [0, 1]}}}},
        ));
        assert!(w.contains("does not take a `window`"), "{op}: {w}");
        let s = refused(set(doc! {"output": {"f": {op: "$v"}}}));
        assert!(s.contains("requires a `sortBy`"), "{op}: {s}");
    }
}

// ---------------------------------------------------------------------------
// Stage-level refusals
// ---------------------------------------------------------------------------

#[test]
fn the_stage_refuses_what_it_does_not_know() {
    let out = |o: Document| doc! {"output": o};
    assert!(refused(set(doc! {"output": {}, "extra": 1})).contains("does not take `extra`"));
    assert!(refused(set(doc! {"partitionBy": "$a"})).contains("needs `output`"));
    assert!(refused(set(doc! {"output": 1})).contains("`output` must be a document"));
    assert!(refused(set(doc! {"output": {"a": 1}})).contains("must be a document naming"));
    assert!(refused(set(doc! {"output": {"a": {}}})).contains("names no window operator"));
    assert!(
        refused(set(doc! {"output": {"a": {"window": {"documents": [0, 1]}}}}))
            .contains("names no window operator")
    );
    assert!(
        refused(set(doc! {"output": {"a": {"$sum": 1, "$avg": 1}}}))
            .contains("two window operators")
    );
    assert!(
        refused(set(doc! {"output": {"a": {"$sum": 1, "other": 1}}}))
            .contains("unknown field `other`")
    );
    let unknown = refused(set(doc! {"output": {"a": {"$median": 1}}}));
    assert!(
        unknown.contains("$median is not a window operator") && unknown.contains("$rank"),
        "{unknown}"
    );
    assert!(
        refused(set(doc! {"output": {"a": {"$mergeObjects": "$x"}}}))
            .contains("not a window operator")
    );
    assert!(refused(set(doc! {"sortBy": {}, "output": {}})).contains("at least one field"));
    assert!(refused(set(doc! {"sortBy": {"a": 2}, "output": {}})).contains("must be 1 or -1"));
    assert!(refused(set(doc! {"sortBy": {"a": "asc"}, "output": {}})).contains("must be 1 or -1"));
    assert!(
        refused(set(doc! {"sortBy": {"a": {"$meta": "textScore"}}, "output": {}}))
            .contains("must be 1 or -1")
    );
    assert!(
        refused(set(doc! {"sortBy": "a", "output": {}})).contains("`sortBy` must be a document")
    );
    assert!(refused(set(out(doc! {"": {"$sum": 1}}))).contains("cannot be empty"));
    assert!(
        refused(set(out(doc! {"$a": {"$sum": 1}}))).contains("cannot be empty, start with `$`")
    );
    assert!(refused(set(out(doc! {"a..b": {"$sum": 1}}))).contains("empty path segment"));
    assert!(
        refused(set(doc! {"output": {"a": {"$sum": 1}}, "partitionBy": {"$nope": 1}}))
            .contains("$nope")
    );
    // The accumulators' own argument rules.
    assert!(
        refused(set(doc! {"output": {"a": {"$count": 1}}})).contains("$count takes no arguments")
    );
    assert!(
        refused(set(doc! {"output": {"a": {"$topN": {"sortBy": {"a": 1}, "output": "$a"}}}}))
            .contains("`n`")
    );
}

#[test]
fn covariance_is_refused_by_name() {
    for op in ["$covariancePop", "$covarianceSamp"] {
        let e = refused(set(doc! {"sortBy": {"a": 1}, "output": {"c": {op: ["$a", "$b"]}}}));
        assert!(e.contains(op) && e.contains("not supported"), "{e}");
    }
}

#[test]
fn the_stage_is_one_document_one_key_and_a_document() {
    assert!(aggregate::parse(&[doc! {"$setWindowFields": 1}]).is_err());
    assert!(aggregate::parse(&[doc! {"$setWindowFields": []}]).is_err());
    let stages = aggregate::parse(&[set(doc! {"output": {}})]).unwrap();
    assert_eq!(stages[0].name(), "$setWindowFields");
    assert!(!stages[0].needs_storage());
}

#[test]
fn an_expression_error_fails_the_stage() {
    let e = refused_run(
        set(doc! {"output": {"a": {"$sum": {"$add": ["$s", 1]}}}}),
        vec![doc! {"s": "text"}],
    );
    assert!(e.contains("$add"), "{e}");
}

// ---------------------------------------------------------------------------
// Inside other stages, and in a pipeline
// ---------------------------------------------------------------------------

#[test]
fn the_stage_runs_inside_a_facet_and_after_other_stages() {
    let pipeline = [
        doc! {"$match": {"state": "CA"}},
        doc! {"$facet": {
            "running": [
                set(doc! {"sortBy": {"day": 1}, "output": {"t": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}}}}),
                doc! {"$project": {"_id": 0, "t": 1}},
            ],
        }},
    ];
    let stages = aggregate::parse(&pipeline).unwrap();
    let mut docs = sales();
    for stage in &stages {
        docs = aggregate::apply(stage, docs, &Limits::default()).unwrap();
    }
    assert_eq!(
        docs,
        vec![doc! {"running": [{"t": 10i64}, {"t": 30i64}, {"t": 60i64}, {"t": 100i64}]}]
    );
}

#[test]
fn the_stage_parses_in_a_lookup_sub_pipeline_and_reads_its_let_variables() {
    let pipeline = [doc! {"$lookup": {
        "from": "other",
        "let": {"scale": "$k"},
        "pipeline": [set(doc! {"sortBy": {"_id": 1}, "output": {"t": {"$sum": {"$multiply": ["$qty", "$$scale"]}, "window": {"documents": ["unbounded", "current"]}}}})],
        "as": "joined",
    }}];
    let stages = aggregate::parse(&pipeline).unwrap();
    let Stage::Lookup { join: aggregate::Join::Pipeline { stages: inner, .. }, .. } = &stages[0]
    else {
        panic!("{:?}", stages[0])
    };
    assert!(matches!(inner[0], Stage::SetWindowFields(_)));
    // The same stage run with the variable bound.
    let scale = Bson::Int32(10);
    let vars = [("scale", &scale)];
    let out = aggregate::apply_with_vars(&inner[0], series(&[1, 2, 3]), &Limits::default(), &vars)
        .unwrap();
    assert_eq!(col(&out, "t"), i64s(&[10, 30, 60]));
}

// ---------------------------------------------------------------------------
// Caps
// ---------------------------------------------------------------------------

fn spec_of(stage: Document) -> window::WindowSpec {
    match aggregate::parse(&[stage]).unwrap().remove(0) {
        Stage::SetWindowFields(spec) => *spec,
        other => panic!("{other:?}"),
    }
}

fn size(doc: &Document) -> usize {
    bson::serialize_to_vec(doc).unwrap().len()
}

#[test]
fn buffered_documents_are_held_to_the_byte_cap_with_an_early_refusal() {
    let input = series(&[1, 2, 3, 4]);
    let each = size(&input[0]);
    let spec = spec_of(set(doc! {"output": {"n": {"$count": {}}}}));
    // Room for three of four documents: refused at the fourth, before anything is computed.
    let err = window::apply_capped(&spec, input.clone(), &Limits::default(), &[], each * 3 + 1)
        .unwrap_err();
    assert!(
        matches!(err, Error::AnswerTooLarge { what: "$setWindowFields", bytes, .. } if bytes == each * 4),
        "{err:?}"
    );
}

#[test]
fn the_values_the_stage_adds_are_held_to_the_byte_cap_exactly() {
    let input = series(&[1, 2, 3]);
    let held: usize = input.iter().map(size).sum();
    // `n` is an Int32 count: type byte, "n", terminator, four bytes.
    let cell = 1 + 1 + 1 + 4;
    let spec = spec_of(set(doc! {"output": {"n": {"$count": {}}}}));
    // The cap is exactly what the documents and three cells come to: allowed.
    let out = window::apply_capped(&spec, input.clone(), &Limits::default(), &[], held + 3 * cell)
        .unwrap();
    assert_eq!(out.len(), 3);
    // One byte less: refused, with the total that passed.
    let err = window::apply_capped(&spec, input, &Limits::default(), &[], held + 3 * cell - 1)
        .unwrap_err();
    assert!(
        matches!(err, Error::AnswerTooLarge { bytes, max, .. } if bytes == held + 3 * cell && max == held + 3 * cell - 1),
        "{err:?}"
    );
}

#[test]
fn a_quadratic_output_is_refused_while_it_is_made_not_after() {
    // A cumulative $push of 2,000 documents is two million values; the cap stops it early.
    let input: Vec<Document> = (0..2000).map(|i| doc! {"_id": i, "v": i}).collect();
    let spec = spec_of(set(
        doc! {"sortBy": {"_id": 1}, "output": {"p": {"$push": "$v", "window": {"documents": ["unbounded", "current"]}}}},
    ));
    let started = std::time::Instant::now();
    let err =
        window::apply_capped(&spec, input, &Limits::default(), &[], 2 * 1024 * 1024).unwrap_err();
    assert!(matches!(err, Error::AnswerTooLarge { .. }), "{err:?}");
    assert!(started.elapsed().as_secs() < 10);
}

#[test]
fn one_answer_given_to_every_document_is_charged_for_every_copy() {
    let input: Vec<Document> = (0..100).map(|i| doc! {"_id": i, "v": "x".repeat(100)}).collect();
    let held: usize = input.iter().map(size).sum();
    let spec = spec_of(set(doc! {"output": {"all": {"$push": "$v"}}}));
    // The array is ~10 KB and goes into 100 documents: a megabyte.
    let err =
        window::apply_capped(&spec, input, &Limits::default(), &[], held + 500_000).unwrap_err();
    assert!(matches!(err, Error::AnswerTooLarge { .. }), "{err:?}");
}

#[test]
fn a_run_inside_the_default_cap_is_not_refused() {
    let out = window::apply(
        &spec_of(set(doc! {"output": {"n": {"$count": {}}}})),
        series(&[1, 2]),
        &Limits::default(),
        &[],
    )
    .unwrap();
    assert_eq!(out.len(), 2);
}

#[test]
fn the_cap_error_is_the_answer_too_large_the_api_maps_to_413() {
    // The API maps this core error to 413; pin the variant the stage raises.
    let spec = spec_of(set(doc! {"output": {"n": {"$count": {}}}}));
    let err = window::apply_capped(&spec, series(&[1]), &Limits::default(), &[], 1).unwrap_err();
    assert!(matches!(err, Error::AnswerTooLarge { .. }));
}
