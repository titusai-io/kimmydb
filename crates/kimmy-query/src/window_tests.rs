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

/// Integers as a window `$sum` answers them: the narrowest width that holds each, an
/// `Int32` while it fits (the reference's windows.diag; the `$group` sum differs, ADR-268).
fn sum_ints(values: &[i64]) -> Vec<Bson> {
    values.iter().map(|v| crate::expr::integer_bson(*v)).collect()
}

fn ints(values: &[i32]) -> Vec<Bson> {
    values.iter().map(|v| Bson::Int32(*v)).collect()
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
    assert_eq!(col(&out, "total"), sum_ints(&[10, 30, 60, 100, 5, 20]));
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
    assert_eq!(col(&out, "total"), sum_ints(&[100, 100, 100, 100, 20, 20]));
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
fn documents_leave_partition_by_partition_in_ascending_order_of_the_key_and_sorted_within() {
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
    // CA before WA though WA arrived first: the partitions are in the order of their keys.
    assert_eq!(ids(&out), vec![4, 2, 3, 1]);
    assert_eq!(col(&out, "n"), ints(&[1, 2, 1, 2]));
    // Descending within each.
    let out = run(
        set(doc! {"partitionBy": "$state", "sortBy": {"day": -1}, "output": {"n": {"$documentNumber": {}}}}),
        input,
    )
    .unwrap();
    assert_eq!(ids(&out), vec![2, 4, 1, 3]);
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
    assert_eq!(col(&out, "t"), sum_ints(&[3, 3, 4]));
}

#[test]
fn a_partition_expression_may_compute_the_key() {
    let out = run(
        set(doc! {"partitionBy": {"$mod": ["$qty", 2]}, "sortBy": {"qty": 1},
        "output": {"t": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}}}}),
        series(&[1, 2, 3, 4, 5]),
    )
    .unwrap();
    // The even (key 0) before the odd (key 1), though an odd one arrived first.
    assert_eq!(ids(&out), vec![2, 4, 1, 3, 5]);
    assert_eq!(col(&out, "t"), sum_ints(&[2, 6, 1, 4, 9]));
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
    assert_eq!(last.get("sum"), Some(&Bson::Int32(0)));
    for field in ["avg", "min", "max", "first", "last", "sd"] {
        assert_eq!(last.get(field), Some(&Bson::Null), "{field}");
    }
    assert_eq!(last.get("push"), Some(&Bson::Array(vec![])));
    assert_eq!(last.get("set"), Some(&Bson::Array(vec![])));
    assert_eq!(last.get("n"), Some(&Bson::Int32(0)));
    // The first document sees the two after it.
    assert_eq!(out[0].get("sum"), Some(&Bson::Int32(5)));
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
    assert_eq!(col(&out, "prev2"), sum_ints(&[0, 1, 3, 7, 14]));
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
    // One value is an integer zero, as the reference answers it.
    assert_eq!(col(&out, "pop")[0], Bson::Int32(0));
    near(&col(&out, "pop")[1..], &[1.0, 0.0, 0.0, 0.5, 0.0, 1.0, 1.0]);
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
    assert_eq!(col(&out, "s"), vec![dec("1.5"), dec("3.5"), Bson::Int32(5)]);
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
        vec![Bson::Int32(1), Bson::Double(1.5), Bson::Double(0.5), Bson::Int32(4)]
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
            assert_eq!(
                o.get("s"),
                // The values are longs: a window of them sums to a long, an empty one to
                // the int 0.
                Some(&if window.is_empty() {
                    Bson::Int32(0)
                } else {
                    Bson::Int64(window.iter().sum())
                }),
                "[{lo},{hi}] at {i}"
            );
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
    assert_eq!(out[0].get("s"), Some(&Bson::Int32(501)));
    assert_eq!(out[30_000].get("s"), Some(&Bson::Int32(1001)));
    assert_eq!(out[n as usize - 1].get("c"), Some(&Bson::Int32(501)));
}

#[test]
fn a_sliding_sum_is_typed_by_its_current_contents_and_stays_cheap() {
    // 100,000 documents and a window of 1,001, a long every 7,000th: a sum is a long
    // exactly while a long is in its window, and an int otherwise, typed by counts as the
    // window slides, not by adding the window up again. (The reference types a sliding sum
    // by its history; see the deviations register.)
    let n = 100_000;
    let input: Vec<Document> = (0..n)
        .map(|i| {
            let v = if i % 7_000 == 0 { Bson::Int64(1) } else { Bson::Int32(1) };
            doc! {"_id": i, "v": v}
        })
        .collect();
    let started = std::time::Instant::now();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$v", "window": {"documents": [-500, 500]}},
        }}),
        input,
    )
    .unwrap();
    assert!(started.elapsed().as_secs() < 20, "took {:?}", started.elapsed());
    let has_long = |i: usize| {
        let (lo, hi) = (i.saturating_sub(500), (i + 500).min(n as usize - 1));
        (lo..=hi).any(|j| j % 7_000 == 0)
    };
    for i in [
        0usize, 499, 500, 501, 3_499, 3_500, 6_499, 6_500, 7_000, 7_500, 7_501, 50_000, 56_000,
        99_999,
    ] {
        let want = if has_long(i) {
            Bson::Int64(sum_len(i, n as usize))
        } else {
            Bson::Int32(sum_len(i, n as usize) as i32)
        };
        assert_eq!(out[i].get("s"), Some(&want), "at {i}");
    }
}

/// The number of documents in the window `[i - 500, i + 500]` of `n`.
fn sum_len(i: usize, n: usize) -> i64 {
    (i.saturating_sub(500)..=(i + 500).min(n - 1)).count() as i64
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
    assert_eq!(col(&out, "qty"), sum_ints(&[1, 3, 6]));
    assert_eq!(col(&out, "again"), sum_ints(&[1, 3, 6]));
}

#[test]
fn an_output_may_be_a_dotted_path() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"stats.running": {"$sum": "$qty", "window": {"documents": ["unbounded", "current"]}}}}),
        vec![doc! {"_id": 1, "qty": 4, "stats": {"keep": true}}, doc! {"_id": 2, "qty": 5}],
    )
    .unwrap();
    assert_eq!(out[0].get_document("stats").unwrap(), &doc! {"keep": true, "running": 4});
    assert_eq!(out[1].get_document("stats").unwrap(), &doc! {"running": 9});
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
    assert_eq!(col(&out, "s"), sum_ints(&[1, 3, 6, 8]));
}

/// Days 1, 1, 2, 2, 3 with quantities 1, 2, 4, 8, 16, so each sum names the documents in its
/// window. The expected sums are what the reference answers.
fn tied_days() -> Vec<Document> {
    [(1, 1), (1, 2), (2, 4), (2, 8), (3, 16)]
        .iter()
        .enumerate()
        .map(|(i, (day, qty))| doc! {"_id": i as i32 + 1, "day": *day, "qty": *qty})
        .collect()
}

#[test]
fn a_numeric_range_bound_includes_the_documents_tied_with_the_current_one_and_current_does_not() {
    let sums = |lo: Bson, hi: Bson| {
        let out = run(
            set(doc! {"sortBy": {"day": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [lo, hi]}}}}),
            tied_days(),
        )
        .unwrap();
        col(&out, "s")
    };
    let b = |s: &str| Bson::String(s.into());
    let n = |x: i32| Bson::Int32(x);
    // A distance of 0 takes every document of the same day.
    assert_eq!(sums(n(0), n(0)), sum_ints(&[3, 3, 12, 12, 16]));
    // The current document itself is the document, ties before it and after it left out.
    assert_eq!(sums(b("current"), b("current")), sum_ints(&[1, 2, 4, 8, 16]));
    assert_eq!(sums(b("unbounded"), b("current")), sum_ints(&[1, 3, 7, 15, 31]));
    assert_eq!(sums(n(-1), b("current")), sum_ints(&[1, 3, 7, 15, 28]));
    assert_eq!(sums(b("current"), b("unbounded")), sum_ints(&[31, 30, 28, 24, 16]));
    assert_eq!(sums(b("current"), n(1)), sum_ints(&[15, 14, 28, 24, 16]));
    assert_eq!(sums(b("current"), n(0)), sum_ints(&[3, 2, 12, 8, 16]));
    assert_eq!(sums(n(0), b("current")), sum_ints(&[1, 3, 4, 12, 16]));
    assert_eq!(sums(n(0), n(1)), sum_ints(&[15, 15, 28, 28, 16]));
    assert_eq!(sums(n(-1), n(0)), sum_ints(&[3, 3, 15, 15, 28]));
    assert_eq!(sums(b("unbounded"), n(0)), sum_ints(&[3, 3, 15, 15, 31]));
    assert_eq!(sums(n(0), b("unbounded")), sum_ints(&[31, 31, 28, 28, 16]));
}

#[test]
fn current_in_a_range_window_is_the_document_for_every_accumulator_and_for_dates() {
    let out = run(
        set(doc! {"sortBy": {"day": 1}, "output": {
            "p": {"$push": "$qty", "window": {"range": ["unbounded", "current"]}},
            "c": {"$count": {}, "window": {"range": ["current", "current"]}},
            "lo": {"$min": "$qty", "window": {"range": ["current", "current"]}},
            "hi": {"$max": "$qty", "window": {"range": ["current", "current"]}},
        }}),
        tied_days(),
    )
    .unwrap();
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    assert_eq!(
        col(&out, "p"),
        vec![
            ints(&[1]),
            ints(&[1, 2]),
            ints(&[1, 2, 4]),
            ints(&[1, 2, 4, 8]),
            ints(&[1, 2, 4, 8, 16])
        ]
    );
    assert_eq!(col(&out, "c"), vec![Bson::Int32(1); 5]);
    assert_eq!(col(&out, "lo"), col(&out, "hi"));
    assert_eq!(col(&out, "hi"), ints_bson(&[1, 2, 4, 8, 16]));
    // The same over dates: [-1 day, current] takes the ties before the current one only.
    let dates: Vec<Document> = [(0, 1), (0, 2), (1, 4), (1, 8)]
        .iter()
        .enumerate()
        .map(|(i, (day, qty))| {
            doc! {"_id": i as i32 + 1, "t": d(&format!("2023-01-0{}T00:00:00Z", day + 1)), "qty": *qty}
        })
        .collect();
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [-1, "current"], "unit": "day"}}}}),
        dates,
    )
    .unwrap();
    assert_eq!(col(&out, "s"), sum_ints(&[1, 3, 7, 15]));
}

fn ints_bson(values: &[i32]) -> Vec<Bson> {
    values.iter().map(|v| Bson::Int32(*v)).collect()
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
    assert_eq!(col(&out, "up"), sum_ints(&[3, 3, 12, 12]));
    // qty 1: from 0 = 12; qty 2: from 1 = 12; qty 4: from 3 = 9; qty 5: from 4 = 9.
    assert_eq!(col(&out, "down"), sum_ints(&[12, 12, 9, 9]));
}

#[test]
fn a_range_window_over_a_descending_sort_is_refused_as_the_reference_refuses_it() {
    for sort in [doc! {"day": -1}, doc! {"t": -1}] {
        let e = refused(set(
            doc! {"sortBy": sort, "output": {"s": {"$sum": 1, "window": {"range": [-3, 0]}}}},
        ));
        assert!(e.contains("ascending `sortBy`"), "{e}");
    }
    // A documents window over a descending sort is fine, and follows the sort.
    let out = run(
        set(doc! {"sortBy": {"day": -1}, "output": {"s": {"$push": "$day", "window": {"documents": [-1, 0]}}}}),
        vec![doc! {"day": 1}, doc! {"day": 2}, doc! {"day": 4}],
    )
    .unwrap();
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    assert_eq!(col(&out, "s"), vec![ints(&[4]), ints(&[4, 2]), ints(&[2, 1])]);
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
    assert_eq!(col(&out, "s"), sum_ints(&[1, 3, 4]));
    // A bound with a unit is a whole number of it, small enough for 32 bits.
    for bound in [Bson::Double(-0.5), Bson::Double(-1e12), Bson::Int64(1 << 40)] {
        let e = refused(set(
            doc! {"sortBy": {"t": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [bound, 0], "unit": "hour"}}}},
        ));
        assert!(e.contains("range bound"), "{e}");
    }
    // A bound without one may be a fraction.
    let out = run(
        set(doc! {"sortBy": {"qty": 1}, "output": {"s": {"$sum": "$qty", "window": {"range": [-0.5, 0]}}}}),
        series(&[1, 2, 4]),
    )
    .unwrap();
    assert_eq!(col(&out, "s"), sum_ints(&[1, 2, 4]));
    let _ = input;
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
    assert_eq!(col(&out, "s"), sum_ints(&[1, 3, 6]));
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
    assert!(bounds(doc! {"unit": "day"}).contains("needs `documents` or `range`"));
    assert!(bounds(doc! {"documents": [0, 1], "range": [0, 1]}).contains("one of"));
    assert!(bounds(doc! {"documents": [0, 1], "unit": "day"}).contains("goes with `range`"));
    assert!(bounds(doc! {"range": [0, 1], "unit": "fortnight"}).contains("`unit` must be one of"));
    assert!(bounds(doc! {"range": [0, 1], "unit": 5}).contains("`unit` must be one of"));
    assert!(bounds(doc! {"range": [0.5, 1], "unit": "month"}).contains("range bound"));
    // A bound is a whole number of 32 bits: the reference refuses a long past that.
    assert!(
        bounds(doc! {"documents": [Bson::Int64(1 << 40), Bson::Int64(1 << 41)]})
            .contains("fits 32 bits")
    );
    // No bound reaches below the lower one.
    assert!(bounds(doc! {"range": ["current", -1]}).contains("must not be above"));
    assert!(bounds(doc! {"range": [1, "current"]}).contains("must not be above"));
    assert!(bounds(doc! {"documents": [0, 1], "extra": 1}).contains("does not take `extra`"));
    let not_doc = refused(set(doc! {"output": {"s": {"$sum": 1, "window": [0, 1]}}}));
    assert!(not_doc.contains("`window` must be a document"), "{not_doc}");
}

#[test]
fn a_documents_window_needs_a_sort_unless_it_is_the_whole_partition() {
    let w = |spec: Document| set(doc! {"output": {"s": {"$sum": 1, "window": spec}}});
    for window in [
        doc! {"documents": [-1, 0]},
        doc! {"documents": ["unbounded", "current"]},
        doc! {"documents": ["current", "unbounded"]},
        doc! {"documents": [0, 0]},
    ] {
        let e = refused(w(window.clone()));
        assert!(e.contains("requires a `sortBy`"), "{window}: {e}");
    }
    // Both bounds unbounded is the whole partition, which needs no order.
    run(w(doc! {"documents": ["unbounded", "unbounded"]}), series(&[1, 2])).unwrap();
    // `{}` is the whole partition too, and `sortBy: {}` is no sort.
    let out = run(w(doc! {}), series(&[1, 2, 3])).unwrap();
    assert_eq!(col(&out, "s"), sum_ints(&[3, 3, 3]));
    // `sortBy: {}` is no sort, and a documents window then counts in arrival order, as the
    // reference accepts it; the same window with no `sortBy` at all is refused.
    let out = run(
        set(doc! {"sortBy": {}, "output": {"s": {"$push": "$qty", "window": {"documents": [-1, 0]}}}}),
        series(&[3, 1, 2]),
    )
    .unwrap();
    let arr = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    assert_eq!(col(&out, "s"), vec![arr(&[3]), arr(&[3, 1]), arr(&[1, 2])]);
    assert_eq!(ids(&out), vec![1, 2, 3]);
    let out = run(set(doc! {"sortBy": {}, "output": {"s": {"$sum": 1}}}), series(&[1, 2])).unwrap();
    assert_eq!(col(&out, "s"), sum_ints(&[2, 2]));
}

#[test]
fn an_infinite_range_bound_is_no_bound() {
    let out = run(
        set(doc! {"sortBy": {"qty": 1}, "output": {
            "lo": {"$sum": "$qty", "window": {"range": [f64::NEG_INFINITY, 0]}},
            "hi": {"$sum": "$qty", "window": {"range": [0, f64::INFINITY]}},
        }}),
        series(&[1, 2, 4]),
    )
    .unwrap();
    assert_eq!(col(&out, "lo"), sum_ints(&[1, 3, 7]));
    assert_eq!(col(&out, "hi"), sum_ints(&[7, 6, 4]));
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
    // Int32, as the reference answers them.
    assert_eq!(col(&out, "rank"), ints(&[1, 2, 2, 4]));
    assert_eq!(col(&out, "dense"), ints(&[1, 2, 2, 3]));
    assert_eq!(col(&out, "n"), ints(&[1, 2, 3, 4]));
}

#[test]
fn rank_restarts_in_each_partition() {
    let input = vec![
        doc! {"_id": 1, "g": "a", "y": 1},
        doc! {"_id": 2, "g": "a", "y": 2},
        doc! {"_id": 3, "g": "a", "y": 2},
        doc! {"_id": 4, "g": "b", "y": 1},
    ];
    let out = run(
        set(doc! {"partitionBy": "$g", "sortBy": {"y": 1}, "output": {"rank": {"$rank": {}}, "dense": {"$denseRank": {}}}}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "rank"), ints(&[1, 2, 2, 1]));
    assert_eq!(col(&out, "dense"), ints(&[1, 2, 2, 1]));
}

#[test]
fn the_rank_family_needs_a_sort_of_exactly_one_field() {
    for op in ["$rank", "$denseRank", "$documentNumber"] {
        let e = refused(set(doc! {"sortBy": {"x": 1, "y": 1}, "output": {"r": {op: {}}}}));
        assert!(e.contains("exactly one field"), "{op}: {e}");
    }
}

#[test]
fn a_missing_sort_value_ranks_apart_from_a_null_one_and_a_one_from_a_one_point_zero() {
    let out = run(
        set(doc! {"sortBy": {"s": 1}, "output": {"rank": {"$rank": {}}, "dense": {"$denseRank": {}}}}),
        vec![
            doc! {"_id": 1, "s": 1},
            doc! {"_id": 2},
            doc! {"_id": 3, "s": Bson::Null},
            doc! {"_id": 4, "s": 1.0},
            doc! {"_id": 5},
        ],
    )
    .unwrap();
    // Null and missing sort together, in arrival order, and rank apart: a missing value is
    // not a null one, and only neighbours tie.
    assert_eq!(ids(&out), vec![2, 3, 5, 1, 4]);
    assert_eq!(col(&out, "rank"), ints(&[1, 2, 3, 4, 4]));
    assert_eq!(col(&out, "dense"), ints(&[1, 2, 3, 4, 4]));
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

/// The default of `$shift` is any constant expression, as the reference accepts (phase B,
/// `q8d-m3.out`: `0` and `{$add: [1, 2]}` answered; a field path and `$rand` refused with
/// "expression must yield a constant value"): it is evaluated with no document in reach.
#[test]
fn shift_takes_any_constant_expression_as_its_default() {
    let defaults = |default: Bson| {
        let out = run(
            set(doc! {"sortBy": {"_id": 1}, "output": {
                "prev": {"$shift": {"output": "$qty", "by": -1, "default": default}},
            }}),
            series(&[10, 20]),
        )
        .unwrap();
        col(&out, "prev")[0].clone()
    };
    assert_eq!(defaults(Bson::Document(doc! {"$add": [1, 2]})), Bson::Int32(3));
    assert_eq!(defaults(Bson::Document(doc! {"$multiply": [2, {"$add": [1, 2]}]})), Bson::Int32(6));
    assert_eq!(defaults(Bson::Document(doc! {"$concat": ["a", "b"]})), Bson::String("ab".into()));
    assert_eq!(defaults(Bson::Document(doc! {"$literal": "$qty"})), Bson::String("$qty".into()));
    assert_eq!(
        defaults(Bson::Document(doc! {"$cond": [true, "yes", "no"]})),
        Bson::String("yes".into())
    );
    assert_eq!(defaults(Bson::Null), Bson::Null);
    assert_eq!(defaults(Bson::String("none".into())), Bson::String("none".into()));
    assert_eq!(defaults(Bson::Int32(0)), Bson::Int32(0));
    assert_eq!(
        defaults(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])),
        Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])
    );
    // A default that reads the document, or draws, is refused.
    let r = |default: Bson| {
        refused(set(doc! {"sortBy": {"a": 1}, "output": {
            "s": {"$shift": {"output": "$a", "by": 1, "default": default}},
        }}))
    };
    for default in [
        Bson::String("$b".into()),
        Bson::String("$$ROOT".into()),
        Bson::String("$$CURRENT.b".into()),
        Bson::Document(doc! {"$add": ["$b", 1]}),
        Bson::Document(doc! {"$concat": ["a", "$b"]}),
        Bson::Document(doc! {"$rand": {}}),
        Bson::Document(doc! {"$add": [1, {"$rand": {}}]}),
    ] {
        assert!(r(default.clone()).contains("constant"), "{default}");
    }
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
    // A `$rand` reads no document and is still no constant.
    assert!(
        r(Bson::Document(doc! {"output": "$a", "by": 1, "default": {"$rand": {}}}))
            .contains("random")
    );
    assert!(
        r(Bson::Document(doc! {"output": "$a", "by": 1, "default": {"$add": [1, {"$rand": {}}]}}))
            .contains("constant")
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
    // An integer zero for one document, as the reference answers it; then
    // (2-1)*(10+20)/2 = 15; 15 + (4-2)*(20+50)/2 = 85.
    assert_eq!(col(&out, "i"), vec![Bson::Int32(0), Bson::Double(15.0), Bson::Double(85.0)]);
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"i": {"$integral": {"input": "$y"}, "window": {"documents": [-1, 0]}}}}),
        xy(),
    )
    .unwrap();
    assert_eq!(col(&out, "i"), vec![Bson::Int32(0), Bson::Double(15.0), Bson::Double(70.0)]);
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
    // 2 hours at a mean of 15; over dates a window of one document is a double zero.
    assert_eq!(col(&out, "i"), vec![Bson::Double(0.0), Bson::Double(30.0)]);
}

#[test]
fn derivative_and_integral_take_a_range_window() {
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"i": {"$integral": {"input": "$y"}, "window": {"range": [-2, 0]}}}}),
        xy(),
    )
    .unwrap();
    // x = 4 reaches back to x = 2: one trapezoid, (4-2)*(20+50)/2.
    assert_eq!(col(&out, "i"), vec![Bson::Int32(0), Bson::Double(15.0), Bson::Double(70.0)]);
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
    // The document with no number answers the average it has, 10 here.
    assert_eq!(
        col(&out, "e"),
        vec![Bson::Double(10.0), Bson::Double(10.0), Bson::Double(20.0), Bson::Double(6.0)]
    );
    // Before there is a number it is null.
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$v", "N": 3}}}}),
        vec![doc! {"_id": 1}, doc! {"_id": 2, "v": "x"}, doc! {"_id": 3, "v": 4}],
    )
    .unwrap();
    assert_eq!(col(&out, "e"), vec![Bson::Null, Bson::Null, Bson::Double(4.0)]);
}

#[test]
fn a_moving_average_updates_as_the_reference_does_to_the_last_digit() {
    // N = 2 is alpha 2/3; 10 + 2/3 * (30 - 10) is 23.333333333333332, where
    // 2/3 * 30 + 1/3 * 10 is 23.333333333333336.
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$v", "N": 2}}}}),
        vec![doc! {"_id": 1, "v": 10}, doc! {"_id": 2, "v": 30}],
    )
    .unwrap();
    assert_eq!(col(&out, "e"), vec![Bson::Double(10.0), Bson::Double(23.333333333333332)]);
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
    }
    let s = refused(set(doc! {"output": {"f": {"$linearFill": "$v"}}}));
    assert!(s.contains("exactly one field"), "{s}");
}

#[test]
fn locf_without_a_sort_carries_forward_in_the_order_the_documents_arrive() {
    let out = run(
        set(doc! {"output": {"f": {"$locf": "$v"}}}),
        vec![
            doc! {"_id": 1, "x": 3, "v": 1},
            doc! {"_id": 2, "x": 1, "v": Bson::Null},
            doc! {"_id": 3, "x": 2, "v": 5},
            doc! {"_id": 4, "x": 0},
        ],
    )
    .unwrap();
    assert_eq!(ids(&out), vec![1, 2, 3, 4]);
    assert_eq!(col(&out, "f"), ints(&[1, 1, 5, 5]));
    // An empty sortBy is no sort.
    let out = run(
        set(doc! {"sortBy": {}, "output": {"f": {"$locf": "$v"}}}),
        vec![doc! {"v": 1}, doc! {}],
    )
    .unwrap();
    assert_eq!(col(&out, "f"), ints(&[1, 1]));
}

#[test]
fn linear_fill_refuses_a_repeated_sort_value() {
    let e = refused_run(
        set(doc! {"sortBy": {"x": 1}, "output": {"f": {"$linearFill": "$v"}}}),
        vec![
            doc! {"x": 1, "v": 1},
            doc! {"x": 2},
            doc! {"x": 2, "v": 5},
            doc! {"x": 3},
            doc! {"x": 5, "v": 9},
        ],
    );
    assert!(e.contains("no repeated values"), "{e}");
}

#[test]
fn linear_fill_interpolates_in_decimal_when_an_end_is_a_decimal() {
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"f": {"$linearFill": "$v"}}}),
        vec![
            doc! {"x": 1, "v": dec("1.1")},
            doc! {"x": 2},
            doc! {"x": 3, "v": dec("2.2")},
            doc! {"x": 4},
            doc! {"x": 6, "v": 10_i64},
        ],
    )
    .unwrap();
    // 1.1 + 1.1 * 1 / 2 and 2.2 + 7.8 * 1 / 3, the product taken before the division.
    assert_eq!(col(&out, "f")[1], dec("1.65"));
    assert_eq!(col(&out, "f")[3], dec("4.8"));
    // Plain numbers are doubles.
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"f": {"$linearFill": "$v"}}}),
        vec![doc! {"x": 1, "v": 1}, doc! {"x": 2}, doc! {"x": 4, "v": 7}],
    )
    .unwrap();
    assert_eq!(col(&out, "f")[1], Bson::Double(3.0));
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
    // `sortBy: {}` is no sort.
    run(set(doc! {"sortBy": {}, "output": {}}), series(&[1])).unwrap();
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

fn pairs() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "x": 1, "y": 2},
        doc! {"_id": 2, "x": 2, "y": 4},
        doc! {"_id": 3, "x": 4, "y": 5},
        doc! {"_id": 4, "x": 5, "y": "z"},
        doc! {"_id": 5, "x": 7, "y": 11},
    ]
}

#[test]
fn covariance_slides_over_pairs_and_skips_a_pair_with_a_member_that_is_not_a_number() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "p": {"$covariancePop": ["$x", "$y"], "window": {"documents": [-2, 0]}},
            "s": {"$covarianceSamp": ["$x", "$y"], "window": {"documents": [-2, 0]}},
        }}),
        pairs(),
    )
    .unwrap();
    // One pair: a sample is null and a population covariance is 0. Then (2,4),(4,5): the
    // x mean 3 and y mean 4.5, so (-1 * -0.5 + 1 * 0.5) = 1 over 2 or 1. Row 4 skips (5, "z").
    // Row 5: (4,5) and (7,11): means 5.5 and 8, (-1.5 * -3 + 1.5 * 3) = 9 over 2 or 1.
    near(&col(&out, "p"), &[0.0, 0.5, 1.4444444444444444, 0.5, 4.5]);
    let s = col(&out, "s");
    assert_eq!(s[0], Bson::Null);
    near(&s[1..], &[1.0, 2.1666666666666665, 1.0, 9.0]);
}

#[test]
fn covariance_over_the_whole_partition_and_over_nothing() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "p": {"$covariancePop": ["$x", {"$multiply": ["$x", 2]}]},
            "s": {"$covarianceSamp": ["$x", {"$multiply": ["$x", 2]}]},
        }}),
        vec![doc! {"x": 1}, doc! {"x": 2}, doc! {"x": 3}],
    )
    .unwrap();
    // y = 2x: the covariance is twice the variance of x, 2/3 population and 1 sample, doubled.
    near(&col(&out, "p")[..1], &[4.0 / 3.0]);
    near(&col(&out, "s")[..1], &[2.0]);
    // No pair at all: null.
    let out = run(
        set(doc! {"output": {"p": {"$covariancePop": ["$a", "$b"]}, "s": {"$covarianceSamp": ["$a", "$b"]}}}),
        vec![doc! {"a": "x", "b": 1}, doc! {"b": 2}],
    )
    .unwrap();
    assert_eq!(col(&out, "p"), vec![Bson::Null; 2]);
    assert_eq!(col(&out, "s"), vec![Bson::Null; 2]);
}

#[test]
fn covariance_with_a_non_finite_member_is_nan_while_it_is_in_the_window_and_not_after() {
    let input = vec![
        doc! {"_id": 1, "x": 1, "y": 1},
        doc! {"_id": 2, "x": 2, "y": f64::INFINITY},
        doc! {"_id": 3, "x": 3, "y": 3},
        doc! {"_id": 4, "x": 4, "y": 5},
        doc! {"_id": 5, "x": 5, "y": 4},
    ];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
        "p": {"$covariancePop": ["$x", "$y"], "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    let p = col(&out, "p");
    assert!(matches!(p[1], Bson::Double(x) if x.is_nan()), "{p:?}");
    assert!(matches!(p[2], Bson::Double(x) if x.is_nan()), "{p:?}");
    // (3,3),(4,5) then (4,5),(5,4): never a stale NaN.
    near(&p[3..], &[0.5, -0.25]);
}

#[test]
fn covariance_refusals() {
    for op in ["$covariancePop", "$covarianceSamp"] {
        for arg in [
            Bson::Array(vec![]),
            Bson::Array(vec!["$a".into()]),
            Bson::String("$a".into()),
            Bson::Document(doc! {"a": 1}),
        ] {
            let e = refused(set(doc! {"sortBy": {"a": 1}, "output": {"c": {op: arg.clone()}}}));
            assert!(e.contains("array of two expressions"), "{op} {arg}: {e}");
        }
    }
    // `$mergeObjects` is not a window operator, as the reference has it.
    let e = refused(set(doc! {"output": {"a": {"$mergeObjects": "$x"}}}));
    assert!(e.contains("not a window operator"), "{e}");
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
    assert_eq!(docs, vec![doc! {"running": [{"t": 10}, {"t": 30}, {"t": 60}, {"t": 100}]}]);
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
    assert_eq!(col(&out, "t"), sum_ints(&[10, 30, 60]));
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

/// Limits whose byte budget is `cap`.
fn capped(cap: usize) -> Limits {
    Limits { buffer: crate::budget::BufferBudget::new(cap), ..Default::default() }
}

/// What a document is charged: the estimate of the memory it holds parsed.
fn size(doc: &Document) -> usize {
    crate::budget::held_bytes(doc)
}

/// What a cell `n`, an `Int32`, written into a document of the two fields `series` makes
/// is charged: its key, and no growth of the table (three fields fit its capacity).
fn n_cell() -> usize {
    crate::budget::held_element("n", &Bson::Int32(0)) + crate::budget::held_frame_growth(2, 1)
}

#[test]
fn buffered_documents_are_held_to_the_byte_cap_with_an_early_refusal() {
    let input = series(&[1, 2, 3, 4]);
    let each = size(&input[0]);
    let spec = spec_of(set(doc! {"output": {"n": {"$count": {}}}}));
    // Room for three of four documents: refused at the fourth, before anything is computed.
    let err = window::apply(&spec, input.clone(), &capped(each * 3 + 1), &[]).unwrap_err();
    assert!(
        matches!(err, Error::ValueTooLarge { what: "the documents $setWindowFields buffers", bytes, .. } if bytes == each * 4),
        "{err:?}"
    );
}

#[test]
fn the_values_the_stage_adds_are_held_to_the_byte_cap_exactly() {
    let input = series(&[1, 2, 3]);
    let held: usize = input.iter().map(size).sum();
    // `n` is an Int32 count: the heap of its key.
    let cell = n_cell();
    let spec = spec_of(set(doc! {"output": {"n": {"$count": {}}}}));
    // The cap is exactly what the documents and three cells come to: allowed.
    let out = window::apply(&spec, input.clone(), &capped(held + 3 * cell), &[]).unwrap();
    assert_eq!(out.len(), 3);
    // One byte less: refused, with the total that passed.
    let err = window::apply(&spec, input, &capped(held + 3 * cell - 1), &[]).unwrap_err();
    assert!(
        matches!(err, Error::ValueTooLarge { bytes, max, .. } if bytes == held + 3 * cell && max == held + 3 * cell - 1),
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
    let err = window::apply(&spec, input, &capped(2 * 1024 * 1024), &[]).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
    assert!(started.elapsed().as_secs() < 10);
}

#[test]
fn one_answer_given_to_every_document_is_charged_for_every_copy() {
    let input: Vec<Document> = (0..100).map(|i| doc! {"_id": i, "v": "x".repeat(100)}).collect();
    let held: usize = input.iter().map(size).sum();
    let spec = spec_of(set(doc! {"output": {"all": {"$push": "$v"}}}));
    // The array is ~10 KB and goes into 100 documents: a megabyte.
    let err = window::apply(&spec, input, &capped(held + 500_000), &[]).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
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
    let err = window::apply(&spec, series(&[1]), &capped(1), &[]).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { .. }));
}

// ---------------------------------------------------------------------------
// Partitions leave in order of their keys
// ---------------------------------------------------------------------------

#[test]
fn partitions_leave_in_the_canonical_order_of_their_values_and_null_and_missing_are_one() {
    let input = vec![
        doc! {"_id": 1, "p": "b"},
        doc! {"_id": 2, "p": 2},
        doc! {"_id": 3, "p": Bson::Null},
        doc! {"_id": 4},
        doc! {"_id": 5, "p": "a"},
        doc! {"_id": 6, "p": 1.0},
        doc! {"_id": 7, "p": true},
        doc! {"_id": 8, "p": 1},
        doc! {"_id": 9, "p": {"x": 1}},
        doc! {"_id": 10, "p": d("2023-01-01T00:00:00Z")},
    ];
    let out = run(set(doc! {"partitionBy": "$p", "output": {"n": {"$count": {}}}}), input).unwrap();
    // null and missing together (in arrival order), then the numbers (1 and 1.0 together), the
    // strings, the document, the bool, the date.
    assert_eq!(ids(&out), vec![3, 4, 6, 8, 2, 5, 1, 9, 7, 10]);
    assert_eq!(col(&out, "n")[..4], ints(&[2, 2, 2, 2])[..]);
}

#[test]
fn a_partition_that_evaluates_to_an_array_is_refused() {
    let e = refused_run(
        set(doc! {"partitionBy": "$p", "output": {"n": {"$count": {}}}}),
        vec![doc! {"p": 1}, doc! {"p": [1, 2]}],
    );
    assert!(e.contains("cannot evaluate to an array"), "{e}");
}

#[test]
fn a_range_window_refuses_an_array_sort_value() {
    let e = refused_run(
        set(
            doc! {"sortBy": {"day": 1}, "output": {"s": {"$sum": 1, "window": {"range": [-1, 0]}}}},
        ),
        vec![doc! {"day": 1}, doc! {"day": [2, 9]}],
    );
    assert!(e.contains("number or a date"), "{e}");
}

// ---------------------------------------------------------------------------
// A missing value
// ---------------------------------------------------------------------------

#[test]
fn first_last_and_shift_leave_the_field_out_for_a_missing_value_and_say_null_for_no_document() {
    let input = vec![doc! {"_id": 1, "v": 1}, doc! {"_id": 2}, doc! {"_id": 3, "v": 3}];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "f": {"$first": "$v", "window": {"documents": [1, 1]}},
            "l": {"$last": "$v", "window": {"documents": [1, 1]}},
            "sh": {"$shift": {"output": "$v", "by": 1}},
        }}),
        input,
    )
    .unwrap();
    // Row 1 looks at the document with no `v`: no field. Row 2 at the one with 3. Row 3 at none.
    assert_eq!(out[0], doc! {"_id": 1, "v": 1});
    assert_eq!(out[1], doc! {"_id": 2, "f": 3, "l": 3, "sh": 3});
    assert_eq!(out[2], doc! {"_id": 3, "v": 3, "f": Bson::Null, "l": Bson::Null, "sh": Bson::Null});
}

#[test]
fn push_and_add_to_set_skip_a_missing_value_and_keep_a_null() {
    let input = vec![
        doc! {"_id": 1, "v": 1},
        doc! {"_id": 2},
        doc! {"_id": 3, "v": Bson::Null},
        doc! {"_id": 4, "v": 1},
    ];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "p": {"$push": "$v", "window": {"documents": ["unbounded", "current"]}},
            "u": {"$addToSet": "$v", "window": {"documents": ["unbounded", "current"]}},
        }}),
        input,
    )
    .unwrap();
    let arr = |v: Vec<Bson>| Bson::Array(v);
    assert_eq!(col(&out, "p")[3], arr(vec![Bson::Int32(1), Bson::Null, Bson::Int32(1)]));
    assert_eq!(col(&out, "u")[3], arr(vec![Bson::Int32(1), Bson::Null]));
    assert_eq!(col(&out, "p")[1], arr(vec![Bson::Int32(1)]));
}

#[test]
fn add_to_set_answers_in_the_order_each_member_first_appears_in_the_window() {
    let ad = vec![3, 1, 3, 2, 1];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "cum": {"$addToSet": "$qty", "window": {"documents": ["unbounded", "current"]}},
            "slide": {"$addToSet": "$qty", "window": {"documents": [-2, 0]}},
        }}),
        series(&ad),
    )
    .unwrap();
    let arr = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    assert_eq!(
        col(&out, "cum"),
        vec![arr(&[3]), arr(&[3, 1]), arr(&[3, 1]), arr(&[3, 1, 2]), arr(&[3, 1, 2])]
    );
    // The window [3, 1, 3] holds 3, 1; then [1, 3, 2]: 1 first now; then [3, 2, 1].
    assert_eq!(
        col(&out, "slide"),
        vec![arr(&[3]), arr(&[3, 1]), arr(&[3, 1]), arr(&[1, 3, 2]), arr(&[3, 2, 1])]
    );
}

/// The reference's mixed-type probe: 1, 1, 1L, "x", null, (missing), [3, 4], 2.5m, 7.
fn mixed() -> Vec<Document> {
    let dec = Bson::Decimal128("2.5".parse().unwrap());
    vec![
        doc! {"_id": 1, "v": 1},
        doc! {"_id": 2, "v": 1},
        doc! {"_id": 3, "v": 1_i64},
        doc! {"_id": 4, "v": "x"},
        doc! {"_id": 5, "v": Bson::Null},
        doc! {"_id": 6},
        doc! {"_id": 7, "v": [3, 4]},
        doc! {"_id": 8, "v": dec},
        doc! {"_id": 9, "v": 7},
    ]
}

#[test]
fn min_and_max_over_mixed_types_keep_the_first_of_equal_values_for_min_and_the_last_for_max() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "lo": {"$min": "$v", "window": {"documents": [-1, 1]}},
            "hi": {"$max": "$v", "window": {"documents": [-1, 1]}},
        }}),
        mixed(),
    )
    .unwrap();
    let dec = Bson::Decimal128("2.5".parse().unwrap());
    let pair = Bson::Array(vec![Bson::Int32(3), Bson::Int32(4)]);
    let x = || Bson::String("x".into());
    assert_eq!(
        col(&out, "lo"),
        vec![
            Bson::Int32(1),
            Bson::Int32(1),
            Bson::Int32(1),
            Bson::Int64(1),
            x(),
            pair.clone(),
            dec.clone(),
            dec.clone(),
            dec.clone()
        ]
    );
    assert_eq!(
        col(&out, "hi"),
        vec![
            Bson::Int32(1),
            Bson::Int64(1),
            x(),
            x(),
            x(),
            pair.clone(),
            pair.clone(),
            pair,
            Bson::Int32(7)
        ]
    );
}

#[test]
fn first_and_last_over_mixed_types_omit_a_missing_value() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "f": {"$first": "$v", "window": {"documents": [0, 1]}},
            "l": {"$last": "$v", "window": {"documents": [-1, 0]}},
        }}),
        mixed(),
    )
    .unwrap();
    // Row 5's first document has a null; row 6's has no value.
    assert_eq!(out[4].get("f"), Some(&Bson::Null));
    assert_eq!(out[4].get("l"), Some(&Bson::Null));
    assert!(!out[5].contains_key("f"), "{:?}", out[5]);
    assert!(!out[5].contains_key("l"), "{:?}", out[5]);
    assert_eq!(out[8].get("f"), Some(&Bson::Int32(7)));
}

#[test]
fn sum_avg_and_count_over_mixed_types_as_the_reference_answers() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$v", "window": {"documents": [-2, 0]}},
            "a": {"$avg": "$v", "window": {"documents": [-2, 0]}},
            "c": {"$count": {}, "window": {"documents": [-2, 0]}},
        }}),
        mixed(),
    )
    .unwrap();
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    assert_eq!(
        col(&out, "s"),
        // The long in rows 3 to 5 makes those windows longs; the reference types a sliding
        // sum by its history and answers the ints 3, 2, 1 here (deviations register).
        vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int64(3),
            Bson::Int64(2),
            Bson::Int64(1),
            Bson::Int32(0),
            Bson::Int32(0),
            dec("2.5"),
            dec("9.5")
        ]
    );
    assert_eq!(
        col(&out, "a"),
        vec![
            Bson::Double(1.0),
            Bson::Double(1.0),
            Bson::Double(1.0),
            Bson::Double(1.0),
            Bson::Double(1.0),
            Bson::Null,
            Bson::Null,
            dec("2.5"),
            dec("4.75")
        ]
    );
    assert_eq!(col(&out, "c")[..3], [Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
}

#[test]
fn push_over_mixed_types_matches_the_reference_where_the_order_is_defined() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
        "p": {"$push": "$v", "window": {"documents": [-1, 1]}}}}),
        mixed(),
    )
    .unwrap();
    let x = || Bson::String("x".into());
    let dec = Bson::Decimal128("2.5".parse().unwrap());
    let pair = Bson::Array(vec![Bson::Int32(3), Bson::Int32(4)]);
    let arr = |v: Vec<Bson>| Bson::Array(v);
    assert_eq!(col(&out, "p")[0], arr(vec![Bson::Int32(1), Bson::Int32(1)]));
    assert_eq!(col(&out, "p")[3], arr(vec![Bson::Int64(1), x(), Bson::Null]));
    // The missing value of row 6 is skipped.
    assert_eq!(col(&out, "p")[4], arr(vec![x(), Bson::Null]));
    assert_eq!(col(&out, "p")[5], arr(vec![Bson::Null, pair.clone()]));
    assert_eq!(col(&out, "p")[6], arr(vec![pair.clone(), dec.clone()]));
    assert_eq!(col(&out, "p")[7], arr(vec![pair, dec, Bson::Int32(7)]));
}

// ---------------------------------------------------------------------------
// Non-finite values, drift and exact sums
// ---------------------------------------------------------------------------

fn infinity_series() -> Vec<Document> {
    [1.0, 2.0, f64::INFINITY, 3.0, 4.0, 5.0, 6.0]
        .iter()
        .enumerate()
        .map(|(i, v)| doc! {"_id": i as i32 + 1, "v": *v})
        .collect()
}

fn is_nan(b: &Bson) -> bool {
    matches!(b, Bson::Double(x) if x.is_nan())
}

#[test]
fn a_sliding_deviation_answers_what_group_answers_over_the_same_window_never_a_stale_nan() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "pop": {"$stdDevPop": "$v", "window": {"documents": [-1, 0]}},
            "samp": {"$stdDevSamp": "$v", "window": {"documents": [-1, 0]}},
        }}),
        infinity_series(),
    )
    .unwrap();
    let pop = col(&out, "pop");
    let samp = col(&out, "samp");
    // While the Infinity is in the window (rows 3 and 4) it is NaN, as `$group` over the same
    // two documents answers; after it has left, the deviation of the two in the window.
    assert_eq!(pop[0], Bson::Int32(0));
    assert_eq!(samp[0], Bson::Null);
    assert!(is_nan(&pop[2]) && is_nan(&pop[3]) && is_nan(&samp[2]) && is_nan(&samp[3]), "{pop:?}");
    near(&pop[4..], &[0.5, 0.5, 0.5]);
    near(&samp[4..], &[2f64.sqrt() / 2.0; 3]);
    // The reference answers null while it is in the window and then 1.89 for every later row.
    // The same documents through `$group`:
    let g = aggregate::apply(
        &aggregate::parse(&[doc! {"$group": {"_id": null, "pop": {"$stdDevPop": "$v"}}}]).unwrap()
            [0],
        vec![doc! {"v": 2.0}, doc! {"v": f64::INFINITY}],
        &Limits::default(),
    )
    .unwrap();
    assert!(is_nan(&g[0].get("pop").cloned().unwrap()));
}

#[test]
fn a_whole_partition_deviation_with_a_nan_is_nan_and_a_lone_non_finite_sample_is_null() {
    let out = run(
        set(doc! {"output": {"pop": {"$stdDevPop": "$v"}, "samp": {"$stdDevSamp": "$v"}}}),
        vec![doc! {"v": 1}, doc! {"v": f64::NAN}, doc! {"v": 3}],
    )
    .unwrap();
    assert!(is_nan(&col(&out, "pop")[0]) && is_nan(&col(&out, "samp")[0]));
    let out = run(
        set(doc! {"output": {"samp": {"$stdDevSamp": "$v"}, "pop": {"$stdDevPop": "$v"}}}),
        vec![doc! {"v": f64::INFINITY}],
    )
    .unwrap();
    assert_eq!(col(&out, "samp"), vec![Bson::Null]);
    assert!(is_nan(&col(&out, "pop")[0]));
}

#[test]
fn a_sliding_deviation_after_thousands_of_steps_is_as_exact_as_one_over_just_the_window() {
    // A common offset of a billion with a spread of a few: subtracting 3,000 times would leave
    // a drift in the last digits that the periodic rebuild takes out.
    let n = 3000usize;
    let values: Vec<f64> = (0..n).map(|i| 1e9 + ((i * 37) % 11) as f64 * 0.3).collect();
    let input: Vec<Document> =
        values.iter().enumerate().map(|(i, v)| doc! {"_id": i as i32, "v": *v}).collect();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
        "pop": {"$stdDevPop": "$v", "window": {"documents": [-2, 0]}}}}),
        input,
    )
    .unwrap();
    let pop = col(&out, "pop");
    for i in [1500usize, 2047, 2048, 2049, 2999] {
        let w = &values[i - 2..=i];
        let mean = w.iter().sum::<f64>() / 3.0;
        let var = w.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / 3.0;
        let Bson::Double(got) = pop[i] else { panic!("{:?}", pop[i]) };
        assert!((got - var.sqrt()).abs() < 1e-7, "row {i}: {got} vs {}", var.sqrt());
    }
}

#[test]
fn sum_and_avg_are_exact_across_a_non_finite_value_and_an_overflow_beyond_a_long() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$v", "window": {"documents": [-1, 0]}},
            "a": {"$avg": "$v", "window": {"documents": [-1, 0]}},
        }}),
        infinity_series(),
    )
    .unwrap();
    let s = col(&out, "s");
    let a = col(&out, "a");
    assert_eq!(s[1], Bson::Double(3.0));
    assert_eq!(s[2], Bson::Double(f64::INFINITY));
    assert_eq!(s[3], Bson::Double(f64::INFINITY));
    assert_eq!(s[4], Bson::Double(7.0));
    assert_eq!(a[2], Bson::Double(f64::INFINITY));
    assert_eq!(a[4], Bson::Double(3.5));
    // A NaN and an opposite infinity.
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"s": {"$sum": "$v", "window": {"documents": [-1, 0]}}}}),
        vec![
            doc! {"_id": 1, "v": f64::INFINITY},
            doc! {"_id": 2, "v": f64::NEG_INFINITY},
            doc! {"_id": 3, "v": 1},
            doc! {"_id": 4, "v": f64::NAN},
            doc! {"_id": 5, "v": 2},
            doc! {"_id": 6, "v": 3},
        ],
    )
    .unwrap();
    let s = col(&out, "s");
    assert_eq!(s[0], Bson::Double(f64::INFINITY));
    assert!(is_nan(&s[1]));
    assert_eq!(s[2], Bson::Double(f64::NEG_INFINITY));
    assert!(is_nan(&s[3]) && is_nan(&s[4]));
    assert_eq!(s[5], Bson::Int32(5));
}

#[test]
fn a_sliding_sum_of_doubles_is_the_exact_sum_rounded_once() {
    let values = [0.1, 0.2, 0.3, 1e16, 1.0, -1e16, 0.7];
    let input: Vec<Document> =
        values.iter().enumerate().map(|(i, v)| doc! {"_id": i as i32, "v": *v}).collect();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {"s": {"$sum": "$v", "window": {"documents": [-2, 0]}}}}),
        input,
    )
    .unwrap();
    // The exact sums of three neighbours, each rounded once: where adding the integer part and
    // the double part separately, or one number at a time, loses the 0.3 or the 1.
    assert_eq!(
        col(&out, "s"),
        f64s(&[0.1, 0.30000000000000004, 0.6, 1e16, 10000000000000002.0, 1.0, -9999999999999998.0])
    );
}

#[test]
fn a_sum_with_a_decimal_is_exact_however_the_window_slides() {
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    let input = vec![
        doc! {"_id": 1, "v": dec("0.1")},
        doc! {"_id": 2, "v": 1},
        doc! {"_id": 3, "v": 2},
        doc! {"_id": 4, "v": 0.5},
        doc! {"_id": 5, "v": 3},
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
        vec![dec("0.1"), dec("1.1"), Bson::Int32(3), Bson::Double(2.5), Bson::Double(3.5)]
    );
    assert_eq!(
        col(&out, "a"),
        vec![dec("0.1"), dec("0.55"), Bson::Double(1.5), Bson::Double(1.25), Bson::Double(1.75)]
    );
}

#[test]
fn an_integral_does_not_lose_small_areas_to_a_large_one_that_has_left_the_window() {
    // A hundred-quadrillion for fifty documents, then ones: windows past the big area are 1.
    let input: Vec<Document> = (0..60)
        .map(|i| doc! {"_id": i + 1, "x": i, "y": if i < 50 { 1e15 } else { 1.0 }})
        .collect();
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {"i": {"$integral": {"input": "$y"}, "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    for row in &out[51..] {
        assert_eq!(row.get("i"), Some(&Bson::Double(1.0)), "{row:?}");
    }
    assert_eq!(out[50].get("i"), Some(&Bson::Double(5e14 + 0.5)));
}

#[test]
fn an_integral_and_a_derivative_over_dates_subtract_the_milliseconds_before_scaling() {
    let input = vec![
        doc! {"t": d("2023-01-01T00:00:00Z"), "y": 10},
        doc! {"t": d("2023-01-01T02:00:00Z"), "y": 20},
        doc! {"t": d("2023-01-01T03:00:00Z"), "y": 50},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {
            "i": {"$integral": {"input": "$y", "unit": "day"}, "window": {"documents": ["unbounded", "current"]}}}}),
        input,
    )
    .unwrap();
    // Not 1.24999999998181: the epoch's 1.67e12 milliseconds are not divided into days first.
    assert_eq!(
        col(&out, "i"),
        vec![Bson::Double(0.0), Bson::Double(1.25), Bson::Double(2.7083333333333335)]
    );
}

// ---------------------------------------------------------------------------
// No window is rebuilt: a hundred thousand documents
// ---------------------------------------------------------------------------

/// Tiny documents, `{_id, v}` with `v` cycling through a thousand values.
fn tiny(n: i32) -> Vec<Document> {
    (0..n).map(|i| doc! {"_id": i, "v": i % 1000}).collect()
}

/// The time to run `output` over `n` tiny documents with the window given, and the output.
fn timed(n: i32, window: Bson, output: Document) -> (std::time::Duration, Vec<Document>) {
    let stage = set(doc! {"sortBy": {"_id": 1}, "output": {"o": merge(output, window)}});
    let input = tiny(n);
    let started = std::time::Instant::now();
    let out = run(stage, input).unwrap();
    (started.elapsed(), out)
}

fn merge(mut op: Document, window: Bson) -> Document {
    op.insert("window", window);
    op
}

const HUNDRED_THOUSAND: i32 = 100_000;

#[test]
fn every_operator_slides_over_a_hundred_thousand_documents_without_rebuilding_the_window() {
    // The shape that was quadratic: the window is half the partition on each side.
    let wide = || Bson::Document(doc! {"documents": [-50_000, 50_000]});
    // And a narrow one, for an operator whose answer is the window.
    let narrow = || Bson::Document(doc! {"documents": [-3, 3]});
    let ops: Vec<(&str, Document, Bson)> = vec![
        ("$addToSet constant", doc! {"$addToSet": 1}, wide()),
        ("$addToSet varied", doc! {"$addToSet": {"$mod": ["$v", 5]}}, wide()),
        ("$maxN 1", doc! {"$maxN": {"n": 1, "input": "$v"}}, wide()),
        ("$minN 3", doc! {"$minN": {"n": 3, "input": "$v"}}, wide()),
        ("$top", doc! {"$top": {"sortBy": {"v": -1}, "output": "$_id"}}, wide()),
        ("$bottom", doc! {"$bottom": {"sortBy": {"v": -1}, "output": "$_id"}}, wide()),
        ("$topN", doc! {"$topN": {"n": 3, "sortBy": {"v": 1}, "output": "$_id"}}, wide()),
        ("$bottomN", doc! {"$bottomN": {"n": 3, "sortBy": {"v": 1}, "output": "$_id"}}, wide()),
        ("$firstN", doc! {"$firstN": {"n": 3, "input": "$v"}}, wide()),
        ("$lastN", doc! {"$lastN": {"n": 3, "input": "$v"}}, wide()),
        ("$push narrow", doc! {"$push": "$v"}, narrow()),
        ("$sum", doc! {"$sum": "$v"}, wide()),
        ("$avg", doc! {"$avg": "$v"}, wide()),
        ("$min", doc! {"$min": "$v"}, wide()),
        ("$max", doc! {"$max": "$v"}, wide()),
        ("$count", doc! {"$count": {}}, wide()),
        ("$stdDevPop", doc! {"$stdDevPop": "$v"}, wide()),
        ("$stdDevSamp", doc! {"$stdDevSamp": "$v"}, wide()),
        ("$covariancePop", doc! {"$covariancePop": ["$v", "$_id"]}, wide()),
        ("$covarianceSamp", doc! {"$covarianceSamp": ["$v", "$_id"]}, wide()),
        ("$first", doc! {"$first": "$v"}, wide()),
        ("$last", doc! {"$last": "$v"}, wide()),
    ];
    for (name, op, window) in ops {
        let (took, out) = timed(HUNDRED_THOUSAND, window, op);
        assert_eq!(out.len(), HUNDRED_THOUSAND as usize, "{name}");
        // Rebuilt for each of 100,000 documents over windows of 50,000 this is billions of steps.
        assert!(took.as_secs() < 30, "{name} took {took:?}");
    }
}

#[test]
fn a_sum_slides_in_time_whether_a_decimal_a_non_finite_value_or_an_overflow_is_in_the_window() {
    let hundred = |value: Bson| -> Vec<Document> {
        (0..HUNDRED_THOUSAND)
            .map(|i| {
                let v =
                    if i % 5000 == 7 { value.clone() } else { Bson::Int64(i64::from(i % 1000)) };
                doc! {"_id": i, "v": v}
            })
            .collect()
    };
    let cases: Vec<(&str, Bson)> = vec![
        ("decimal", Bson::Decimal128("1.5".parse().unwrap())),
        ("nan", Bson::Double(f64::NAN)),
        ("infinity", Bson::Double(f64::INFINITY)),
        ("beyond a long", Bson::Int64(i64::MAX)),
    ];
    for (name, value) in cases {
        for op in ["$sum", "$avg"] {
            let stage = set(doc! {"sortBy": {"_id": 1}, "output": {
            "o": {op: "$v", "window": {"documents": [-50_000, 50_000]}}}});
            let input = hundred(value.clone());
            let started = std::time::Instant::now();
            let out = run(stage, input).unwrap();
            let took = started.elapsed();
            assert_eq!(out.len(), HUNDRED_THOUSAND as usize, "{name} {op}");
            assert!(took.as_secs() < 30, "{name} {op} took {took:?}");
        }
    }
    // Cumulative, the shape that took 135 s at 40,000.
    let stage = set(doc! {"sortBy": {"_id": 1}, "output": {
    "o": {"$sum": "$v", "window": {"documents": ["unbounded", "current"]}}}});
    let started = std::time::Instant::now();
    let out = run(stage, hundred(Bson::Decimal128("1.5".parse().unwrap()))).unwrap();
    assert_eq!(out.len(), HUNDRED_THOUSAND as usize);
    assert!(started.elapsed().as_secs() < 30, "{:?}", started.elapsed());
}

#[test]
fn the_incremental_answers_are_the_answers_over_the_window_for_every_operator() {
    // A window of 3 on each side over a small series, against the answer from the documents of
    // each window computed on its own, with every operator that has a state.
    let n = 40;
    let input: Vec<Document> = (0..n)
        .map(|i| doc! {"_id": i, "v": ((i * 7919) % 23) as i64 - 8, "w": (i * 31) % 5})
        .collect();
    let spec = |op: Document| {
        set(
            doc! {"sortBy": {"_id": 1}, "output": {"o": merge(op, Bson::Document(doc! {"documents": [-3, 2]}))}},
        )
    };
    let whole = |op: Document, docs: &[Document]| -> Bson {
        let out = run(set(doc! {"output": {"o": op}}), docs.to_vec()).unwrap();
        out[0].get("o").cloned().unwrap_or(Bson::Null)
    };
    for op in [
        doc! {"$push": "$v"},
        doc! {"$addToSet": "$w"},
        doc! {"$top": {"sortBy": {"w": 1}, "output": "$v"}},
        doc! {"$bottom": {"sortBy": {"w": 1}, "output": "$v"}},
        doc! {"$topN": {"n": 3, "sortBy": {"w": -1}, "output": "$_id"}},
        doc! {"$bottomN": {"n": 3, "sortBy": {"w": -1}, "output": "$_id"}},
        doc! {"$firstN": {"n": 2, "input": "$v"}},
        doc! {"$lastN": {"n": 2, "input": "$v"}},
        doc! {"$maxN": {"n": 3, "input": "$v"}},
        doc! {"$minN": {"n": 3, "input": "$v"}},
        doc! {"$min": "$v"},
        doc! {"$max": "$v"},
        doc! {"$sum": "$v"},
    ] {
        let out = run(spec(op.clone()), input.clone()).unwrap();
        for (i, row) in out.iter().enumerate() {
            let s = i.saturating_sub(3);
            let e = (i + 3).min(n as usize);
            let expected = whole(op.clone(), &input[s..e]);
            let got = row.get("o").cloned().unwrap_or(Bson::Null);
            // `$addToSet` has no defined order; compare as sets.
            if op.contains_key("$addToSet") {
                let (Bson::Array(mut a), Bson::Array(mut b)) = (got, expected) else { panic!() };
                a.sort_by(kimmy_core::cmp::canonical_cmp);
                b.sort_by(kimmy_core::cmp::canonical_cmp);
                assert_eq!(a, b, "{op} row {i}");
            } else {
                assert_eq!(got, expected, "{op} row {i}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The byte budget
// ---------------------------------------------------------------------------

#[test]
fn the_refusal_names_the_stage_and_its_buffer_limit_not_one_document() {
    let spec = spec_of(set(doc! {"output": {"n": {"$count": {}}}}));
    let err = window::apply(&spec, series(&[1]), &capped(1), &[]).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("$setWindowFields"), "{text}");
    assert!(text.contains("aggregate.memory_limit_bytes"), "{text}");
    assert!(!text.contains("of one document"), "{text}");
    let spec = match aggregate::parse(&[
        doc! {"$fill": {"sortBy": {"x": 1}, "output": {"v": {"method": "locf"}}}},
    ])
    .unwrap()
    .remove(0)
    {
        Stage::Fill(spec) => *spec,
        other => panic!("{other:?}"),
    };
    let window = spec.window.expect("a method");
    let err = window::apply(&window, vec![doc! {"x": 1}], &capped(1), &[]).unwrap_err();
    assert!(err.to_string().starts_with("the documents $fill buffers"), "{err}");
}

#[test]
fn the_sort_keys_taken_are_charged() {
    let input = series(&[1, 2, 3]);
    let held: usize = input.iter().map(size).sum();
    // Each Int32 key is a slot of its own; and the `$documentNumber` output is a cell, an
    // Int32 named `n`.
    let key = crate::budget::held_bytes_bson(&Bson::Int32(0));
    let cell = n_cell();
    let spec = spec_of(set(doc! {"sortBy": {"qty": 1}, "output": {"n": {"$documentNumber": {}}}}));
    let exact = held + 3 * key + 3 * cell;
    assert_eq!(window::apply(&spec, input.clone(), &capped(exact), &[]).unwrap().len(), 3);
    let err = window::apply(&spec, input, &capped(exact - 1), &[]).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { bytes, .. } if bytes == exact), "{err:?}");
}

#[test]
fn the_stages_of_one_pipeline_and_the_branches_of_a_facet_share_one_budget() {
    let input = series(&[1, 2, 3, 4, 5]);
    let held: usize = input.iter().map(size).sum();
    let cell = n_cell();
    let one = held + 5 * cell;
    let stage = set(doc! {"output": {"n": {"$count": {}}}});
    // One stage fits, and so do two in a row: the first gives its bytes back when it is done.
    // (The second reads documents that now hold the first one's field, and adds its own.)
    let limits = capped(held + 10 * cell + 5);
    let stages = aggregate::parse(&[stage.clone(), stage.clone()]).unwrap();
    let mut docs = input.clone();
    for s in &stages {
        docs = aggregate::apply(s, docs, &limits).unwrap();
    }
    assert_eq!(docs.len(), 5);
    assert_eq!(limits.buffer.used(), 0);
    // A `$facet` holds its input while its first branch runs on a copy of it, and the
    // branch's stage holds the copy and what it adds: all of it is charged to the one
    // budget, so a cap that fits the stage alone does not fit the facet.
    let facet = aggregate::parse(&[doc! {"$facet": {
        "a": [stage.clone(), {"$count": "c"}],
        "b": [stage.clone(), {"$count": "c"}],
    }}])
    .unwrap();
    let limits = capped(one + 10);
    let err = aggregate::apply(&facet[0], input.clone(), &limits).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
    // With room for both they run.
    let limits = capped(2 * one + 10);
    assert_eq!(aggregate::apply(&facet[0], input, &limits).unwrap().len(), 1);
    assert_eq!(limits.buffer.used(), 0);
}

// ---------------------------------------------------------------------------
// Pins for the states that the incremental check above cannot see
// ---------------------------------------------------------------------------

#[test]
fn last_n_and_first_n_answer_the_edges_of_the_window() {
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "last2": {"$lastN": {"n": 2, "input": "$qty"}, "window": {"documents": ["unbounded", "current"]}},
            "first2": {"$firstN": {"n": 2, "input": "$qty"}, "window": {"documents": [-2, 0]}},
        }}),
        series(&[10, 40, 20, 30]),
    )
    .unwrap();
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|x| Bson::Int32(*x)).collect());
    assert_eq!(
        col(&out, "last2"),
        vec![ints(&[10]), ints(&[10, 40]), ints(&[40, 20]), ints(&[20, 30])]
    );
    assert_eq!(
        col(&out, "first2"),
        vec![ints(&[10]), ints(&[10, 40]), ints(&[10, 40]), ints(&[40, 20])]
    );
}

#[test]
fn two_adjacent_missing_sort_values_tie_for_a_rank() {
    let out = run(
        set(doc! {"sortBy": {"s": 1}, "output": {"rank": {"$rank": {}}, "dense": {"$denseRank": {}}}}),
        vec![doc! {"_id": 1}, doc! {"_id": 2}, doc! {"_id": 3, "s": 5}],
    )
    .unwrap();
    assert_eq!(col(&out, "rank"), ints(&[1, 1, 3]));
    assert_eq!(col(&out, "dense"), ints(&[1, 1, 2]));
}

#[test]
fn a_double_sum_rounds_half_to_even_and_keeps_a_bit_below_the_half() {
    // 10^16 is a multiple of 2 in doubles. 10^16 + 1 is exactly half way between it and the
    // next double, and goes to the even one; with a tiny positive term more it is above half
    // way and goes up; with a tiny negative term it is below half way and stays.
    let sum = |values: Vec<Bson>| {
        let input: Vec<Document> =
            values.into_iter().enumerate().map(|(i, v)| doc! {"_id": i as i32, "v": v}).collect();
        let out = run(set(doc! {"output": {"s": {"$sum": "$v"}}}), input).unwrap();
        out[0].get("s").cloned().unwrap()
    };
    let d = Bson::Double;
    assert_eq!(sum(vec![d(1e16), d(1.0)]), d(1e16));
    assert_eq!(sum(vec![d(1e16), d(1.0), d(1e-30)]), d(1e16 + 2.0));
    assert_eq!(sum(vec![d(1e16), d(1.0), d(-1e-30)]), d(1e16));
    assert_eq!(sum(vec![d(-1e16), d(-1.0), d(-1e-30)]), d(-1e16 - 2.0));
}

#[test]
fn a_sum_of_integers_past_two_to_the_53_and_a_double_is_exact() {
    let input = vec![doc! {"_id": 1, "v": (1_i64 << 53) + 1}, doc! {"_id": 2, "v": 0.5}];
    let out = run(set(doc! {"output": {"s": {"$sum": "$v"}}}), input).unwrap();
    // 2^53 + 1.5 is nearer 2^53 + 2 than 2^53.
    assert_eq!(col(&out, "s")[0], Bson::Double(9007199254740994.0));
}

#[test]
fn a_deviation_and_a_covariance_over_a_window_that_slides_without_a_rebuild_are_exact() {
    // A window of 20 over 30 documents: ten removals, too few for a rebuild.
    let n = 30usize;
    let xs: Vec<f64> = (0..n).map(|i| ((i * 37) % 11) as f64 * 0.5 + (i as f64) * 0.1).collect();
    let ys: Vec<f64> = (0..n).map(|i| ((i * 53) % 13) as f64 * 0.25 - (i as f64) * 0.2).collect();
    let input: Vec<Document> =
        (0..n).map(|i| doc! {"_id": i as i32, "x": xs[i], "y": ys[i]}).collect();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "sd": {"$stdDevPop": "$x", "window": {"documents": [-19, 0]}},
            "ss": {"$stdDevSamp": "$x", "window": {"documents": [-19, 0]}},
            "cp": {"$covariancePop": ["$x", "$y"], "window": {"documents": [-19, 0]}},
            "cs": {"$covarianceSamp": ["$x", "$y"], "window": {"documents": [-19, 0]}},
        }}),
        input,
    )
    .unwrap();
    for i in 0..n {
        let s = i.saturating_sub(19);
        let (wx, wy) = (&xs[s..=i], &ys[s..=i]);
        let k = wx.len() as f64;
        let (mx, my) = (wx.iter().sum::<f64>() / k, wy.iter().sum::<f64>() / k);
        let vx: f64 = wx.iter().map(|x| (x - mx) * (x - mx)).sum();
        let c: f64 = wx.iter().zip(wy).map(|(x, y)| (x - mx) * (y - my)).sum();
        let get = |name: &str| match out[i].get(name) {
            Some(Bson::Double(v)) => Some(*v),
            _ => None,
        };
        if wx.len() > 1 {
            assert!((get("sd").unwrap() - (vx / k).sqrt()).abs() < 1e-9, "sd at {i}");
            assert!((get("ss").unwrap() - (vx / (k - 1.0)).sqrt()).abs() < 1e-9, "ss at {i}");
            assert!((get("cs").unwrap() - c / (k - 1.0)).abs() < 1e-9, "cs at {i}");
        }
        assert!((get("cp").unwrap() - c / k).abs() < 1e-9, "cp at {i}");
    }
}

#[test]
fn a_deviation_is_rebuilt_from_a_window_that_has_moved_far_from_where_it_started() {
    // A hundred values near 10^12 and then values near 1, in a window of 20: the running
    // sums about the first value would carry 10^12 for good and lose the spread; a rebuild
    // starts again about a value in the window.
    let n = 300usize;
    let values: Vec<f64> = (0..n)
        .map(|i| if i < 100 { 1e12 + (i % 7) as f64 } else { 1.0 + ((i * 13) % 9) as f64 * 0.1 })
        .collect();
    let input: Vec<Document> =
        values.iter().enumerate().map(|(i, v)| doc! {"_id": i as i32, "v": *v}).collect();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
        "sd": {"$stdDevPop": "$v", "window": {"documents": [-19, 0]}}}}),
        input,
    )
    .unwrap();
    for i in [250usize, 299] {
        let w = &values[i - 19..=i];
        let m = w.iter().sum::<f64>() / 20.0;
        let sd = (w.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / 20.0).sqrt();
        let Bson::Double(got) = out[i].get("sd").cloned().unwrap() else { panic!() };
        assert!((got - sd).abs() < 1e-9, "row {i}: {got} vs {sd}");
    }
}

// ---------------------------------------------------------------------------
// Doubles near the largest: an exact sum that can overflow and come back
// ---------------------------------------------------------------------------

mod exact_doubles {
    use crate::window_acc::ExactDoubles;

    fn sum(values: &[f64]) -> f64 {
        let mut acc = ExactDoubles::default();
        for v in values {
            acc.add(*v);
        }
        acc.value()
    }

    #[test]
    fn it_is_the_exact_sum_rounded_once_against_an_integer_oracle() {
        // Multiples of 2^-20 below 2^40: their sum is an integer in i128, and rounding that once
        // to a double and scaling by a power of two is the correctly rounded sum.
        let mut x: u64 = 99;
        let mut next = || {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x >> 11
        };
        for round in 0..200 {
            let mut acc = ExactDoubles::default();
            let mut held: Vec<i64> = Vec::new();
            let mut total: i128 = 0;
            for step in 0..60 {
                if step % 3 == 2 && !held.is_empty() {
                    let k = held.remove(0);
                    acc.add(-(k as f64) / 1048576.0);
                    total -= i128::from(k);
                } else {
                    let k = (next() % (1 << 51)) as i64 - (1 << 50);
                    acc.add(k as f64 / 1048576.0);
                    held.push(k);
                    total += i128::from(k);
                }
                assert_eq!(acc.value(), total as f64 / 1048576.0, "round {round} step {step}");
            }
        }
    }

    #[test]
    fn a_sum_past_the_largest_double_is_an_infinity_and_comes_back_exactly() {
        let a = f64::MAX;
        assert_eq!(sum(&[a, a]), f64::INFINITY);
        assert_eq!(sum(&[-a, -a]), f64::NEG_INFINITY);
        assert_eq!(sum(&[a, a, -a]), a);
        assert_eq!(sum(&[a, a, -a, -a]), 0.0);
        assert_eq!(sum(&[a, -a, 1.0]), 1.0);
        // Half an ulp of the largest double goes to even, which is past it; less stays.
        assert_eq!(sum(&[a, 2f64.powi(970)]), f64::INFINITY);
        assert_eq!(sum(&[a, 2f64.powi(969)]), a);
        // The same through adding and taking away.
        let mut acc = ExactDoubles::default();
        acc.add(a);
        acc.add(a);
        assert_eq!(acc.value(), f64::INFINITY);
        acc.add(-a);
        assert_eq!(acc.value(), a);
    }

    #[test]
    fn subnormals_and_the_smallest_normals_are_exact() {
        let tiny = f64::from_bits(1);
        assert_eq!(sum(&[tiny, tiny, tiny]), 3.0 * tiny);
        assert_eq!(sum(&[f64::MIN_POSITIVE, -tiny]), f64::MIN_POSITIVE - tiny);
        assert_eq!(sum(&[tiny, -tiny]), 0.0);
        // Two halves of the smallest normal.
        assert_eq!(sum(&[f64::MIN_POSITIVE / 2.0, f64::MIN_POSITIVE / 2.0]), f64::MIN_POSITIVE);
        // An integer added to doubles.
        let mut acc = ExactDoubles::default();
        acc.add(0.5);
        assert_eq!(acc.value_with(i128::from(i64::MAX)), 9223372036854775808.0);
        assert_eq!(acc.value_with(-3), -2.5);
    }
}

fn big_pattern(n: i32, f: impl Fn(i32) -> f64) -> Vec<Document> {
    (0..n).map(|i| doc! {"_id": i, "v": f(i)}).collect()
}

#[test]
fn a_sum_and_an_average_of_doubles_near_the_largest_slide_exactly_and_in_time() {
    let a = 1.7e308;
    // A, -A, 1: any three neighbours sum to exactly 1, whatever order they come in.
    let input = big_pattern(HUNDRED_THOUSAND, |i| match i % 3 {
        0 => a,
        1 => -a,
        _ => 1.0,
    });
    let started = std::time::Instant::now();
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$v", "window": {"documents": [-2, 0]}},
            "m": {"$avg": "$v", "window": {"documents": [-2, 0]}},
        }}),
        input,
    )
    .unwrap();
    assert!(started.elapsed().as_secs() < 30, "{:?}", started.elapsed());
    for i in [2usize, 3, 4, 5, 50_000, 99_999] {
        assert_eq!(out[i].get("s"), Some(&Bson::Double(1.0)), "row {i}");
        assert_eq!(out[i].get("m"), Some(&Bson::Double(1.0 / 3.0)), "row {i}");
    }
    // Past the largest and back: A, A, -A, -A in pairs.
    let input = big_pattern(HUNDRED_THOUSAND, |i| if (i / 2) % 2 == 0 { a } else { -a });
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
        "s": {"$sum": "$v", "window": {"documents": [-1, 0]}}}}),
        input,
    )
    .unwrap();
    let s = col(&out, "s");
    // Rows 1, 2, 3, 4: (A, A), (A, -A), (-A, -A), (-A, A).
    assert_eq!(s[1], Bson::Double(f64::INFINITY));
    assert_eq!(s[2], Bson::Double(0.0));
    assert_eq!(s[3], Bson::Double(f64::NEG_INFINITY));
    assert_eq!(s[4], Bson::Double(0.0));
    assert_eq!(s[99_998], Bson::Double(0.0));
    assert_eq!(s[99_999], Bson::Double(f64::NEG_INFINITY));
}

#[test]
fn an_integral_of_values_near_the_largest_slides_exactly_and_in_time() {
    // 8e307 for fifty thousand documents, then ones. Three areas of 8e307 are past the largest
    // double, so a window of four documents is an infinity while they are in it and the exact
    // sum once they have left.
    let input: Vec<Document> = (0..HUNDRED_THOUSAND)
        .map(|i| doc! {"_id": i, "x": i, "y": if i < 50_000 { 8e307 } else { 1.0 }})
        .collect();
    let started = std::time::Instant::now();
    let out = run(
        set(doc! {"sortBy": {"x": 1}, "output": {
            "i2": {"$integral": {"input": "$y"}, "window": {"documents": [-1, 0]}},
            "i4": {"$integral": {"input": "$y"}, "window": {"documents": [-3, 0]}},
        }}),
        input,
    )
    .unwrap();
    assert!(started.elapsed().as_secs() < 30, "{:?}", started.elapsed());
    assert_eq!(out[1000].get("i2"), Some(&Bson::Double(8e307)));
    assert_eq!(out[1000].get("i4"), Some(&Bson::Double(f64::INFINITY)));
    assert_eq!(out[99_999].get("i2"), Some(&Bson::Double(1.0)));
    assert_eq!(out[99_999].get("i4"), Some(&Bson::Double(3.0)));
}

// ---------------------------------------------------------------------------
// The #278 delta leftovers (d1 to d6)
// ---------------------------------------------------------------------------

#[test]
fn a_decimal_running_sum_takes_a_value_out_when_it_leaves_the_window() {
    // The sum is decimal once a `Decimal128` is in the window, and a number that leaves
    // it, a decimal or not, comes off the running sum: 1.5, 3.5, 5.5, 7.75, 4.75.
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    let input = vec![
        doc! {"_id": 1, "qty": dec("1.5")},
        doc! {"_id": 2, "qty": 2},
        doc! {"_id": 3, "qty": dec("3.5")},
        doc! {"_id": 4, "qty": 4.25},
        doc! {"_id": 5, "qty": dec("0.5")},
    ];
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "s": {"$sum": "$qty", "window": {"documents": [-1, 0]}},
            "a": {"$avg": "$qty", "window": {"documents": [-1, 0]}},
        }}),
        input,
    )
    .unwrap();
    let value = |b: &Bson| match b {
        Bson::Decimal128(d) => d.to_string().parse::<f64>().unwrap(),
        other => panic!("{other:?} is not a decimal"),
    };
    let sums: Vec<f64> = col(&out, "s").iter().map(value).collect();
    assert_eq!(sums, vec![1.5, 3.5, 5.5, 7.75, 4.75]);
    let avgs: Vec<f64> = col(&out, "a").iter().map(value).collect();
    assert_eq!(avgs, vec![1.5, 1.75, 2.75, 3.875, 2.375]);
}

#[test]
fn a_derivative_over_dates_subtracts_the_dates_before_it_scales_them() {
    // One second apart, in hours: the difference is worked out exactly in milliseconds and
    // then scaled. Scaled first, each date is a number near 4.6e5 hours with an error of
    // 6e-11, which is 2e-7 of the difference.
    let input = vec![
        doc! {"t": d("2023-01-01T00:00:00.001Z"), "y": 10},
        doc! {"t": d("2023-01-01T00:00:01.001Z"), "y": 20},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {
            "d": {"$derivative": {"input": "$y", "unit": "hour"}, "window": {"documents": [-1, 0]}},
            "i": {"$integral": {"input": "$y", "unit": "hour"}, "window": {"documents": [-1, 0]}},
        }}),
        input,
    )
    .unwrap();
    let hours = 1000.0 / 3_600_000.0;
    assert_eq!(col(&out, "d")[1], Bson::Double(10.0 / hours));
    assert_eq!(col(&out, "i")[1], Bson::Double(hours * (10.0 + 20.0) / 2.0));
}

#[test]
fn two_dates_more_than_two_to_the_sixty_three_milliseconds_apart_do_not_wrap() {
    // The span of the earliest and the latest date a `DateTime` holds is 2^64 ms: in 64 bits
    // it wraps to a small or negative number.
    let input = vec![
        doc! {"t": bson::DateTime::from_millis(i64::MIN), "y": 0.0},
        doc! {"t": bson::DateTime::from_millis(i64::MAX), "y": 1.0},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {
            "d": {"$derivative": {"input": "$y", "unit": "millisecond"}, "window": {"documents": [-1, 0]}},
        }}),
        input,
    )
    .unwrap();
    let Bson::Double(slope) = col(&out, "d")[1] else { panic!("{out:?}") };
    let span = 2f64.powi(64);
    assert!((slope - 1.0 / span).abs() < 1e-25, "{slope}");
}

/// The moving average as the reference answers it (q8a2 n6): kept in decimal, to 34 digits, so
/// that `N: 2` over `[1, 2]` is `1.6666666666666667` where a double's
/// `prev + alpha * (x - prev)` is `1.6666666666666665`. Every answer below is a recorded one.
#[test]
fn the_moving_average_is_worked_in_decimal_as_the_reference_works_it() {
    let ema = |arg: Document, input: Vec<Document>| {
        let out =
            run(set(doc! {"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": arg}}}), input)
                .unwrap();
        col(&out, "e")
    };
    let v = |values: &[i32]| -> Vec<Document> {
        values.iter().enumerate().map(|(i, x)| doc! {"_id": i as i32 + 1, "v": *x}).collect()
    };
    // N: 2 over [1, 2].
    assert_eq!(ema(doc! {"input": "$v", "N": 2}, v(&[1, 2])), f64s(&[1.0, 1.6666666666666667]));
    // A longer series, N: 2 over 1, 2, 3, 4 (wextra3, "sortBy {} with $expMovingAvg").
    assert_eq!(
        ema(doc! {"input": "$v", "N": 2}, v(&[1, 2, 3, 4])),
        f64s(&[1.0, 1.6666666666666667, 2.5555555555555554, 3.5185185185185186])
    );
    // A missing value and a string leave the average where it is, and a leading missing
    // value answers null; N given as a whole double (wextra, "ema N 2.0 double").
    let sparse = vec![
        doc! {"_id": 1},
        doc! {"_id": 2, "v": 10},
        doc! {"_id": 3, "v": "x"},
        doc! {"_id": 4, "v": 30},
    ];
    assert_eq!(
        ema(doc! {"input": "$v", "N": 2.0}, sparse.clone()),
        vec![Bson::Null, Bson::Double(10.0), Bson::Double(10.0), Bson::Double(23.333333333333332)]
    );
    // The alpha form (wextra, "ema leading missing, string, decimal", and windows, "ema alpha").
    assert_eq!(
        ema(doc! {"input": "$v", "alpha": 0.5}, sparse),
        vec![Bson::Null, Bson::Double(10.0), Bson::Double(10.0), Bson::Double(20.0)]
    );
    assert_eq!(
        ema(doc! {"input": "$v", "alpha": 0.25}, v(&[10, 20, 30])),
        f64s(&[10.0, 12.5, 16.875])
    );
    assert_eq!(ema(doc! {"input": "$v", "N": 3}, v(&[10, 20, 30])), f64s(&[10.0, 15.0, 22.5]));
    // A long series is the same decimal walk: N: 4 (alpha 0.4) over a ramp, checked against
    // the same arithmetic done on exact decimals here.
    let ramp: Vec<i32> = (1..=40).collect();
    let got = ema(doc! {"input": "$v", "N": 4}, v(&ramp));
    let mut want = Vec::new();
    let mut prev = 0i128; // exact, in units of 1e-30; alpha = 2/5
    for (i, x) in ramp.iter().enumerate() {
        let x = i128::from(*x) * 10i128.pow(30);
        prev = if i == 0 { x } else { (x * 2 + prev * 3) / 5 };
        want.push(Bson::Double(prev as f64 / 1e30));
    }
    for (g, w) in got.iter().zip(&want) {
        let (Bson::Double(g), Bson::Double(w)) = (g, w) else { panic!("{got:?}") };
        assert!((g - w).abs() <= 1e-12 * w.abs().max(1.0), "{g} vs {w}");
    }
}

#[test]
fn an_integral_over_dates_more_than_two_to_the_sixty_three_milliseconds_apart_does_not_wrap() {
    // The trapezoid between the earliest and the latest date a `DateTime` holds is 2^64
    // milliseconds wide and 1 high: in 64 bits the width wraps to -1.
    let input = vec![
        doc! {"t": bson::DateTime::from_millis(i64::MIN), "y": 0.0},
        doc! {"t": bson::DateTime::from_millis(i64::MAX), "y": 2.0},
    ];
    let out = run(
        set(doc! {"sortBy": {"t": 1}, "output": {
            "i": {"$integral": {"input": "$y", "unit": "millisecond"}, "window": {"documents": [-1, 0]}},
        }}),
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "i")[1], Bson::Double(2f64.powi(64)));
}

#[test]
fn sort_by_nothing_is_accepted_by_shift_and_the_moving_average_in_arrival_order() {
    let input =
        vec![doc! {"_id": 1, "qty": 10}, doc! {"_id": 3, "qty": 30}, doc! {"_id": 2, "qty": 20}];
    let out = run(
        set(doc! {"sortBy": {}, "output": {
            "next": {"$shift": {"output": "$qty", "by": 1, "default": -1}},
            "ema": {"$expMovingAvg": {"input": "$qty", "alpha": 0.5}},
        }}),
        input,
    )
    .unwrap();
    // Arrival order: 1, 3, 2.
    assert_eq!(ids(&out), vec![1, 3, 2]);
    assert_eq!(col(&out, "next"), ints(&[30, 20, -1]));
    assert_eq!(col(&out, "ema"), f64s(&[10.0, 20.0, 20.0]));
    // With no `sortBy` at all they are still refused.
    let e = refused(set(doc! {"output": {"n": {"$shift": {"output": "$qty", "by": 1}}}}));
    assert!(e.contains("requires a `sortBy`"), "{e}");
    let e =
        refused(set(doc! {"output": {"n": {"$expMovingAvg": {"input": "$qty", "alpha": 0.5}}}}));
    assert!(e.contains("requires a `sortBy`"), "{e}");
}

#[test]
fn a_deviation_of_one_value_is_a_double_zero_over_the_whole_partition_and_an_int_in_a_window() {
    // The reference computes a window that starts at the partition's first document with the
    // plain accumulator, which answers a double zero for one value, and any other window with
    // one that can take a document out, which answers an integer zero.
    let out = run(
        set(doc! {"sortBy": {"_id": 1}, "output": {
            "whole": {"$stdDevPop": "$qty"},
            "all": {"$stdDevPop": "$qty", "window": {"documents": ["unbounded", "unbounded"]}},
            "cumulative": {"$stdDevPop": "$qty", "window": {"documents": ["unbounded", "current"]}},
            "sliding": {"$stdDevPop": "$qty", "window": {"documents": [-1, 0]}},
            "alone": {"$stdDevPop": "$qty", "window": {"documents": [0, 0]}},
        }}),
        series(&[5]),
    )
    .unwrap();
    for field in ["whole", "all", "cumulative"] {
        assert_eq!(col(&out, field), vec![Bson::Double(0.0)], "{field}");
    }
    for field in ["sliding", "alone"] {
        assert_eq!(col(&out, field), vec![Bson::Int32(0)], "{field}");
    }
}

#[test]
fn fill_orders_a_missing_partition_before_a_null_one() {
    let input = vec![
        doc! {"_id": 1, "g": Bson::Null, "v": 1},
        doc! {"_id": 2, "v": 2},
        doc! {"_id": 3, "g": 5, "v": 3},
        doc! {"_id": 4, "g": Bson::Null, "v": Bson::Null},
        doc! {"_id": 5, "v": Bson::Null},
    ];
    let stage = aggregate::parse(&[doc! {"$fill": {
        "partitionByFields": ["g"], "sortBy": {"_id": 1}, "output": {"v": {"method": "locf"}},
    }}])
    .unwrap()
    .remove(0);
    let out = aggregate::apply(&stage, input, &Limits::default()).unwrap();
    // The partitions leave in the reference's order: missing, null, 5.
    assert_eq!(ids(&out), vec![2, 5, 1, 4, 3]);
    assert_eq!(
        col(&out, "v"),
        vec![Bson::Int32(2), Bson::Int32(2), Bson::Int32(1), Bson::Int32(1), Bson::Int32(3)]
    );
}

// ---------------------------------------------------------------------------
// Integer widths (ADR-268): the reference's recorded answers, classic and SBE alike
// ---------------------------------------------------------------------------

const LMAX: i64 = i64::MAX;

/// The `s` of each document of `values` under `spec`, in `_id` order.
fn widths(values: Vec<Bson>, spec: Document) -> Vec<Bson> {
    let input: Vec<Document> =
        values.into_iter().enumerate().map(|(k, v)| doc! {"_id": k as i32 + 1, "v": v}).collect();
    let out = run(set(doc! {"sortBy": {"_id": 1}, "output": {"s": spec}}), input).unwrap();
    col(&out, "s")
}

fn cumulative() -> Document {
    doc! {"documents": ["unbounded", "current"]}
}

#[test]
fn a_cumulative_or_whole_partition_sum_is_typed_as_a_group_sum_is() {
    let sum = |window: Option<Document>| match window {
        Some(w) => doc! {"$sum": "$v", "window": w},
        None => doc! {"$sum": "$v"},
    };
    let (i, l) = (Bson::Int32, Bson::Int64);
    // Ints only: typed by the total (`1, 3, MAX + 3` is the int 1, the int 3, a long).
    assert_eq!(
        widths(vec![i(1), i(2), i(i32::MAX)], sum(Some(cumulative()))),
        vec![i(1), i(3), l(2_147_483_650)]
    );
    // A sum that saw a long stays a long, whatever the total.
    assert_eq!(widths(vec![l(1), l(2)], sum(Some(cumulative()))), vec![l(1), l(3)]);
    assert_eq!(widths(vec![i(1), l(2)], sum(Some(cumulative()))), vec![i(1), l(3)]);
    assert_eq!(widths(vec![i(1), i(2)], sum(None)), vec![i(3), i(3)]);
    assert_eq!(widths(vec![l(1), l(2)], sum(None)), vec![l(3), l(3)]);
    // With a double, a double.
    assert_eq!(
        widths(vec![i(1), Bson::Double(2.0)], sum(Some(cumulative()))),
        vec![i(1), Bson::Double(3.0)]
    );
    assert_eq!(
        widths(vec![i(i32::MAX)], doc! {"$sum": {"$add": ["$v", 1, -1]}}),
        vec![i(i32::MAX)]
    );
    assert_eq!(
        widths(vec![i(5), i(6)], doc! {"$sum": 1, "window": cumulative()}),
        vec![i(1), i(2)]
    );
    assert_eq!(
        widths(vec![i(5), i(6)], doc! {"$count": {}, "window": cumulative()}),
        vec![i(1), i(2)]
    );
}

#[test]
fn a_sliding_sum_is_typed_by_what_the_window_holds_now() {
    // The reference types a sliding sum by its history: `[LMAX, 1, 2]` over `[-1, 0]` is a
    // long, a DOUBLE, then the int 3. That turns integer inputs into a double, and is not
    // copied (deviations register): here the third window holds the longs 1 and 2, a long.
    let (i, l) = (Bson::Int32, Bson::Int64);
    let slide = doc! {"$sum": "$v", "window": {"documents": [-1, 0]}};
    assert_eq!(
        widths(vec![l(LMAX), l(1), l(2)], slide.clone()),
        vec![l(LMAX), Bson::Double(9_223_372_036_854_775_808.0), l(3)]
    );
    // An int window after a long has left is typed by its total again.
    assert_eq!(widths(vec![l(1), i(2), i(3)], slide.clone()), vec![l(1), l(3), i(5)]);
    // A double leaving the window leaves ints.
    assert_eq!(
        widths(vec![Bson::Double(0.5), i(1), i(2)], slide),
        vec![Bson::Double(0.5), Bson::Double(1.5), i(3)]
    );
}

#[test]
fn the_other_window_functions_have_the_references_widths() {
    let (i, l, d) = (Bson::Int32, Bson::Int64, Bson::Double);
    assert_eq!(
        widths(vec![i(1), i(2)], doc! {"$avg": "$v", "window": cumulative()}),
        vec![d(1.0), d(1.5)]
    );
    for rank in ["$rank", "$denseRank", "$documentNumber"] {
        // Sorted by `_id`, so no ties: 1, 2, 3 as ints.
        assert_eq!(
            widths(vec![i(1), i(1), i(2)], doc! {rank: {}}),
            vec![i(1), i(2), i(3)],
            "{rank}"
        );
    }
    assert_eq!(
        widths(vec![l(7), l(8)], doc! {"$shift": {"output": "$v", "by": 1, "default": 0}}),
        vec![l(8), i(0)]
    );
    assert_eq!(
        widths(vec![l(7), l(8)], doc! {"$min": "$v", "window": cumulative()}),
        vec![l(7), l(7)]
    );
    assert_eq!(
        widths(vec![i(1), i(2), i(3)], doc! {"$integral": {"input": "$v"}, "window": cumulative()}),
        vec![i(0), d(1.5), d(4.0)]
    );
    assert_eq!(
        widths(
            vec![i(1), i(2), i(4)],
            doc! {"$derivative": {"input": "$v"}, "window": {"documents": [-1, 0]}}
        ),
        vec![Bson::Null, d(1.0), d(2.0)]
    );
    // Typed only: the last digit of the second value differs from the reference's (it
    // answers 1.6666666666666667; this build 1.6666666666666665), which is `$expMovingAvg`'s
    // own business and not a width.
    let ema = widths(vec![i(1), i(2)], doc! {"$expMovingAvg": {"input": "$v", "N": 2}});
    assert!(ema.iter().all(|v| matches!(v, Bson::Double(_))), "{ema:?}");
    assert_eq!(
        widths(vec![i(1), Bson::Null, i(3)], doc! {"$linearFill": "$v"}),
        vec![i(1), d(2.0), i(3)]
    );
    assert_eq!(widths(vec![l(1), Bson::Null], doc! {"$locf": "$v"}), vec![l(1), l(1)]);
}
