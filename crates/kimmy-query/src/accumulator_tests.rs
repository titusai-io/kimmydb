//! The accumulators of ADR-262: what they answer, and what they refuse.

use bson::{Bson, Decimal128, Document, doc};

use crate::accumulators;
use crate::aggregate::{self, Limits};

fn run_with(
    stage: Document,
    input: Vec<Document>,
    limits: &Limits,
) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&[stage])?;
    aggregate::apply(&stages[0], input, limits)
}

fn group(fields: Document, input: Vec<Document>) -> Document {
    let mut spec = doc! {"_id": null};
    spec.extend(fields);
    let out = run_with(doc! {"$group": spec}, input, &Limits::default()).unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    out.into_iter().next().unwrap()
}

fn one(acc: Document, input: Vec<Document>) -> Bson {
    group(doc! {"r": acc}, input).get("r").unwrap().clone()
}

fn refused(acc: Document) -> String {
    aggregate::parse(&[doc! {"$group": {"_id": null, "r": acc}}])
        .expect_err("must be refused")
        .to_string()
}

fn xs(values: &[i32]) -> Vec<Document> {
    values.iter().enumerate().map(|(i, x)| doc! {"_id": i as i32, "x": *x}).collect()
}

fn f(v: &Bson) -> f64 {
    match v {
        Bson::Double(d) => *d,
        other => panic!("not a double: {other:?}"),
    }
}

fn close(a: f64, b: f64) {
    assert!((a - b).abs() <= 1e-12 * b.abs().max(1.0), "{a} vs {b}");
}

// ---------------------------------------------------------------------------
// $stdDevPop, $stdDevSamp
// ---------------------------------------------------------------------------

#[test]
fn std_dev_of_a_plain_set() {
    // 2 4 4 4 5 5 7 9: mean 5, population variance 4.
    let data = xs(&[2, 4, 4, 4, 5, 5, 7, 9]);
    assert_eq!(one(doc! {"$stdDevPop": "$x"}, data.clone()), Bson::Double(2.0));
    close(f(&one(doc! {"$stdDevSamp": "$x"}, data)), (32.0f64 / 7.0).sqrt());
}

#[test]
fn std_dev_counts_only_numbers() {
    let data = vec![
        doc! {"x": 1},
        doc! {"x": "9"},
        doc! {"x": null},
        doc! {"y": 1},
        doc! {"x": true},
        doc! {"x": [5]},
        doc! {"x": 3i64},
        doc! {"x": 5.0},
    ];
    // Counted: 1, 3, 5.
    close(f(&one(doc! {"$stdDevSamp": "$x"}, data.clone())), 2.0);
    close(f(&one(doc! {"$stdDevPop": "$x"}, data)), (8.0f64 / 3.0).sqrt());
}

#[test]
fn std_dev_of_nothing_numeric_is_null() {
    let data = vec![doc! {"x": "a"}, doc! {"x": null}, doc! {"y": 1}];
    assert_eq!(one(doc! {"$stdDevPop": "$x"}, data.clone()), Bson::Null);
    assert_eq!(one(doc! {"$stdDevSamp": "$x"}, data), Bson::Null);
}

#[test]
fn std_dev_of_one_value_is_zero_for_the_population_and_null_for_a_sample() {
    let data = vec![doc! {"x": 7}, doc! {"x": "no"}];
    assert_eq!(one(doc! {"$stdDevPop": "$x"}, data.clone()), Bson::Double(0.0));
    assert_eq!(one(doc! {"$stdDevSamp": "$x"}, data), Bson::Null);
}

#[test]
fn std_dev_of_a_decimal_is_a_double() {
    let d = |s: &str| Bson::Decimal128(s.parse::<Decimal128>().unwrap());
    let data = vec![doc! {"x": d("1.5")}, doc! {"x": d("2.5")}, doc! {"x": 3.5}];
    let r = one(doc! {"$stdDevPop": "$x"}, data);
    close(f(&r), (2.0f64 / 3.0).sqrt());
}

#[test]
fn std_dev_survives_a_large_offset() {
    // A naive sum-of-squares loses every digit here; the answer is that of 4, 7, 13, 16.
    let base = 1.0e9;
    let data: Vec<Document> = [4.0, 7.0, 13.0, 16.0].iter().map(|v| doc! {"x": base + v}).collect();
    close(f(&one(doc! {"$stdDevSamp": "$x"}, data.clone())), 30.0f64.sqrt());
    close(f(&one(doc! {"$stdDevPop": "$x"}, data)), 22.5f64.sqrt());
}

#[test]
fn std_dev_remove_undoes_an_add() {
    let spec = accumulators::parse("$stdDevPop", &Bson::String("$x".into()), &[]).unwrap().unwrap();
    let limits = Limits::default();
    let mut state = spec.init();
    let docs = xs(&[1, 5, 9, 20]);
    for d in &docs {
        state.accumulate(&spec, &crate::expr::Scope::new(d), &limits).unwrap();
    }
    assert!(state.remove(&spec, &crate::expr::Scope::new(&docs[3])).unwrap());
    assert!(state.remove(&spec, &crate::expr::Scope::new(&docs[0])).unwrap());
    // 5 and 9 remain.
    close(f(&state.finish()), 2.0);
}

// ---------------------------------------------------------------------------
// $count
// ---------------------------------------------------------------------------

#[test]
fn count_counts_every_document_including_those_without_the_field() {
    let data = vec![doc! {"x": 1}, doc! {"y": 1}, doc! {"x": null}];
    assert_eq!(one(doc! {"$count": {}}, data), Bson::Int32(3));
}

#[test]
fn count_takes_no_arguments() {
    assert!(refused(doc! {"$count": {"a": 1}}).contains("$count takes no arguments"));
    assert!(refused(doc! {"$count": "$x"}).contains("$count takes no arguments"));
}

// ---------------------------------------------------------------------------
// $top, $bottom, $topN, $bottomN
// ---------------------------------------------------------------------------

fn scores() -> Vec<Document> {
    vec![
        doc! {"name": "a", "score": 10},
        doc! {"name": "b", "score": 30},
        doc! {"name": "c", "score": 20},
        doc! {"name": "d", "score": 30},
        doc! {"name": "e", "score": 5},
    ]
}

#[test]
fn top_and_bottom_answer_one_output() {
    assert_eq!(
        one(doc! {"$top": {"sortBy": {"score": -1}, "output": "$name"}}, scores()),
        Bson::String("b".into())
    );
    assert_eq!(
        one(doc! {"$bottom": {"sortBy": {"score": -1}, "output": "$name"}}, scores()),
        Bson::String("e".into())
    );
    assert_eq!(
        one(doc! {"$top": {"sortBy": {"score": 1}, "output": ["$name", "$score"]}}, scores()),
        Bson::Array(vec![Bson::String("e".into()), Bson::Int32(5)])
    );
}

#[test]
fn top_n_and_bottom_n_answer_arrays_in_sort_order() {
    assert_eq!(
        one(doc! {"$topN": {"n": 3, "sortBy": {"score": -1}, "output": "$name"}}, scores()),
        Bson::Array(vec!["b".into(), "d".into(), "c".into()])
    );
    // The bottom three of the descending order, still in that order.
    assert_eq!(
        one(doc! {"$bottomN": {"n": 3, "sortBy": {"score": -1}, "output": "$name"}}, scores()),
        Bson::Array(vec!["c".into(), "a".into(), "e".into()])
    );
}

#[test]
fn ties_keep_arrival_order() {
    // b and d tie at 30: the earlier is first, so $top is b and $bottom (ascending) is d.
    assert_eq!(
        one(doc! {"$top": {"sortBy": {"score": -1}, "output": "$name"}}, scores()),
        Bson::String("b".into())
    );
    assert_eq!(
        one(doc! {"$bottom": {"sortBy": {"score": 1}, "output": "$name"}}, scores()),
        Bson::String("d".into())
    );
}

#[test]
fn sort_by_several_keys() {
    let r = one(
        doc! {"$topN": {"n": 2, "sortBy": {"score": -1, "name": -1}, "output": "$name"}},
        scores(),
    );
    assert_eq!(r, Bson::Array(vec!["d".into(), "b".into()]));
}

#[test]
fn missing_sort_values_sort_as_null_and_missing_output_is_null() {
    let data = vec![doc! {"score": 2, "n": "x"}, doc! {"n": "y"}, doc! {"score": 1}];
    let r = one(doc! {"$topN": {"n": 3, "sortBy": {"score": 1}, "output": "$n"}}, data);
    assert_eq!(r, Bson::Array(vec!["y".into(), Bson::Null, "x".into()]));
}

#[test]
fn sort_values_of_mixed_types_follow_the_canonical_order() {
    let data = vec![doc! {"v": "s", "i": 1}, doc! {"v": 3, "i": 2}, doc! {"v": true, "i": 3}];
    let r = one(doc! {"$topN": {"n": 3, "sortBy": {"v": 1}, "output": "$i"}}, data);
    assert_eq!(r, Bson::Array(vec![Bson::Int32(2), Bson::Int32(1), Bson::Int32(3)]));
}

#[test]
fn an_array_sorts_by_its_smallest_ascending_and_largest_descending() {
    let data = vec![doc! {"v": [1, 9], "i": 1}, doc! {"v": [5, 6], "i": 2}];
    let asc = one(doc! {"$top": {"sortBy": {"v": 1}, "output": "$i"}}, data.clone());
    let desc = one(doc! {"$top": {"sortBy": {"v": -1}, "output": "$i"}}, data);
    assert_eq!((asc, desc), (Bson::Int32(1), Bson::Int32(1)));
}

#[test]
fn n_larger_than_the_group_answers_every_row() {
    assert_eq!(
        one(doc! {"$topN": {"n": 50, "sortBy": {"score": 1}, "output": "$score"}}, scores()),
        Bson::Array(vec![5.into(), 10.into(), 20.into(), 30.into(), 30.into()])
    );
}

#[test]
fn n_of_one_is_an_array_of_one() {
    assert_eq!(
        one(doc! {"$bottomN": {"n": 1, "sortBy": {"score": 1}, "output": "$name"}}, scores()),
        Bson::Array(vec!["d".into()])
    );
}

#[test]
fn top_and_bottom_argument_errors() {
    assert!(refused(doc! {"$top": {"output": "$x"}}).contains("`sortBy`"));
    assert!(refused(doc! {"$top": {"sortBy": {"x": 1}}}).contains("`output`"));
    assert!(refused(doc! {"$top": {"sortBy": {}, "output": "$x"}}).contains("at least one"));
    assert!(
        refused(doc! {"$top": {"sortBy": {"x": 2}, "output": "$x"}}).contains("sort direction")
    );
    assert!(
        refused(doc! {"$top": {"n": 2, "sortBy": {"x": 1}, "output": "$x"}})
            .contains("unknown argument: n")
    );
    assert!(refused(doc! {"$topN": {"sortBy": {"x": 1}, "output": "$x"}}).contains("`n`"));
    assert!(refused(doc! {"$topN": 5}).contains("object"));
}

// ---------------------------------------------------------------------------
// $firstN, $lastN, $maxN, $minN
// ---------------------------------------------------------------------------

#[test]
fn first_n_and_last_n_keep_arrival_order_and_nulls() {
    let data = vec![doc! {"x": 1}, doc! {"y": 0}, doc! {"x": 3}, doc! {"x": 4}];
    assert_eq!(
        one(doc! {"$firstN": {"n": 3, "input": "$x"}}, data.clone()),
        Bson::Array(vec![1.into(), Bson::Null, 3.into()])
    );
    assert_eq!(
        one(doc! {"$lastN": {"n": 3, "input": "$x"}}, data),
        Bson::Array(vec![Bson::Null, 3.into(), 4.into()])
    );
}

#[test]
fn max_n_and_min_n_skip_null_and_order_best_first() {
    let data = vec![
        doc! {"x": 5},
        doc! {"x": null},
        doc! {"y": 1},
        doc! {"x": 9},
        doc! {"x": 1},
        doc! {"x": 7},
    ];
    assert_eq!(
        one(doc! {"$maxN": {"n": 3, "input": "$x"}}, data.clone()),
        Bson::Array(vec![9.into(), 7.into(), 5.into()])
    );
    assert_eq!(
        one(doc! {"$minN": {"n": 3, "input": "$x"}}, data.clone()),
        Bson::Array(vec![1.into(), 5.into(), 7.into()])
    );
    // n above the number of values: all of them, nulls still skipped.
    assert_eq!(
        one(doc! {"$minN": {"n": 10, "input": "$x"}}, data),
        Bson::Array(vec![1.into(), 5.into(), 7.into(), 9.into()])
    );
}

#[test]
fn max_n_over_mixed_types_uses_the_canonical_order() {
    let data = vec![doc! {"x": 5}, doc! {"x": "a"}, doc! {"x": true}, doc! {"x": 7}];
    assert_eq!(
        one(doc! {"$maxN": {"n": 2, "input": "$x"}}, data),
        Bson::Array(vec![Bson::Boolean(true), "a".into()])
    );
}

#[test]
fn n_forms_with_n_of_one() {
    let data = xs(&[3, 1, 2]);
    assert_eq!(
        one(doc! {"$firstN": {"n": 1, "input": "$x"}}, data.clone()),
        Bson::Array(vec![3.into()])
    );
    assert_eq!(
        one(doc! {"$lastN": {"n": 1, "input": "$x"}}, data.clone()),
        Bson::Array(vec![2.into()])
    );
    assert_eq!(
        one(doc! {"$maxN": {"n": 1, "input": "$x"}}, data.clone()),
        Bson::Array(vec![3.into()])
    );
    assert_eq!(one(doc! {"$minN": {"n": 1, "input": "$x"}}, data), Bson::Array(vec![1.into()]));
}

#[test]
fn n_must_be_a_positive_integer() {
    for (n, text) in [
        (Bson::Int32(0), "greater than 0"),
        (Bson::Int32(-2), "greater than 0"),
        (Bson::Double(2.5), "must be an integer"),
        (Bson::String("3".into()), "must be an integer"),
        (Bson::Null, "must be an integer"),
    ] {
        let err = refused(doc! {"$firstN": {"n": n.clone(), "input": "$x"}});
        assert!(err.contains(text), "{n:?}: {err}");
    }
    // A field path is missing for the whole group, so it is refused.
    assert!(refused(doc! {"$maxN": {"n": "$k", "input": "$x"}}).contains("must be an integer"));
    // A whole double and a computed constant are fine.
    let data = xs(&[1, 2, 3]);
    assert_eq!(
        one(doc! {"$firstN": {"n": 2.0, "input": "$x"}}, data.clone()),
        Bson::Array(vec![1.into(), 2.into()])
    );
    assert_eq!(
        one(doc! {"$firstN": {"n": {"$add": [1, 1]}, "input": "$x"}}, data),
        Bson::Array(vec![1.into(), 2.into()])
    );
    assert!(refused(doc! {"$firstN": {"input": "$x"}}).contains("`n`"));
    assert!(refused(doc! {"$firstN": {"n": 1}}).contains("`input`"));
    assert!(
        refused(doc! {"$firstN": {"n": 1, "input": "$x", "z": 1}}).contains("unknown argument: z")
    );
}

// ---------------------------------------------------------------------------
// Caps
// ---------------------------------------------------------------------------

#[test]
fn an_n_over_the_cap_is_refused_at_first_use() {
    let limits = Limits { max_documents: 10 };
    for acc in [
        doc! {"$firstN": {"n": 11, "input": "$x"}},
        doc! {"$lastN": {"n": 11, "input": "$x"}},
        doc! {"$maxN": {"n": 11, "input": "$x"}},
        doc! {"$minN": {"n": 11, "input": "$x"}},
        doc! {"$topN": {"n": 11, "sortBy": {"x": 1}, "output": "$x"}},
        doc! {"$bottomN": {"n": 11, "sortBy": {"x": 1}, "output": "$x"}},
    ] {
        let err = run_with(doc! {"$group": {"_id": null, "r": acc}}, xs(&[1]), &limits)
            .expect_err("over the cap");
        assert!(matches!(err, kimmy_core::Error::Limit(_)), "{err}");
        assert!(err.to_string().contains("n = 11"), "{err}");
    }
    // At the cap it works.
    let out = run_with(
        doc! {"$group": {"_id": null, "r": {"$firstN": {"n": 10, "input": "$x"}}}},
        xs(&[1]),
        &limits,
    )
    .unwrap();
    assert_eq!(out[0].get_array("r").unwrap().len(), 1);
}

#[test]
fn rows_held_across_groups_are_charged_to_the_cap() {
    // Each group's n is within the cap; together the buffers are not.
    let limits = Limits { max_documents: 10 };
    let input: Vec<Document> = (0..12).map(|i| doc! {"g": i % 3, "x": i}).collect();
    let err = run_with(
        doc! {"$group": {"_id": "$g", "r": {"$firstN": {"n": 10, "input": "$x"}}}},
        input.clone(),
        &limits,
    )
    .expect_err("12 rows held, cap 10");
    assert!(matches!(err, kimmy_core::Error::Limit(_)), "{err}");
    // A buffer that stays at n rows does not grow past it.
    let out = run_with(
        doc! {"$group": {"_id": "$g", "r": {"$firstN": {"n": 2, "input": "$x"}}}},
        input,
        &Limits { max_documents: 10 },
    )
    .unwrap();
    assert_eq!(out.len(), 3);
}

// ---------------------------------------------------------------------------
// Whole-stage behaviour
// ---------------------------------------------------------------------------

#[test]
fn an_empty_input_has_no_group() {
    let out = run_with(
        doc! {"$group": {"_id": null, "s": {"$stdDevPop": "$x"}, "c": {"$count": {}}}},
        vec![],
        &Limits::default(),
    )
    .unwrap();
    assert!(out.is_empty());
}

#[test]
fn a_group_whose_values_are_all_skipped_answers_empty_arrays_and_null() {
    let data = vec![doc! {"y": 1}, doc! {"x": null}];
    let r = group(
        doc! {
            "mx": {"$maxN": {"n": 2, "input": "$x"}},
            "sd": {"$stdDevSamp": "$x"},
        },
        data,
    );
    assert_eq!(r, doc! {"_id": null, "mx": [], "sd": null});
}

#[test]
fn several_accumulators_in_one_group_with_keys() {
    let input = vec![
        doc! {"c": "p", "x": 2, "n": "a"},
        doc! {"c": "q", "x": 10, "n": "b"},
        doc! {"c": "p", "x": 4, "n": "c"},
        doc! {"c": "p", "x": 6, "n": "d"},
    ];
    let out = run_with(
        doc! {"$group": {
            "_id": "$c",
            "n": {"$count": {}},
            "sum": {"$sum": "$x"},
            "sd": {"$stdDevPop": "$x"},
            "top": {"$top": {"sortBy": {"x": -1}, "output": "$n"}},
            "low": {"$minN": {"n": 2, "input": "$x"}},
            "first": {"$firstN": {"n": 2, "input": "$n"}},
            "last": {"$lastN": {"n": 2, "input": "$n"}},
        }},
        input,
        &Limits::default(),
    )
    .unwrap();
    let p = &out[0];
    assert_eq!(p.get("_id"), Some(&Bson::String("p".into())));
    assert_eq!(p.get("n"), Some(&Bson::Int32(3)));
    assert_eq!(p.get("sum"), Some(&Bson::Int64(12)));
    close(f(p.get("sd").unwrap()), (8.0f64 / 3.0).sqrt());
    assert_eq!(p.get("top"), Some(&Bson::String("d".into())));
    assert_eq!(p.get("low"), Some(&Bson::Array(vec![2.into(), 4.into()])));
    assert_eq!(p.get("first"), Some(&Bson::Array(vec!["a".into(), "c".into()])));
    assert_eq!(p.get("last"), Some(&Bson::Array(vec!["c".into(), "d".into()])));
    let q = &out[1];
    assert_eq!(q.get("sd"), Some(&Bson::Double(0.0)));
    assert_eq!(q.get("n"), Some(&Bson::Int32(1)));
}

#[test]
fn they_work_in_bucket() {
    let out = run_with(
        doc! {"$bucket": {
            "groupBy": "$x", "boundaries": [0, 10, 100],
            "output": {"n": {"$count": {}}, "big": {"$maxN": {"n": 1, "input": "$x"}}},
        }},
        xs(&[1, 2, 50]),
        &Limits::default(),
    )
    .unwrap();
    assert_eq!(out[0], doc! {"_id": 0, "n": 2, "big": [2]});
    assert_eq!(out[1], doc! {"_id": 10, "n": 1, "big": [50]});
}

#[test]
fn an_unknown_accumulator_lists_the_supported_ones() {
    let err = refused(doc! {"$median": "$x"});
    assert!(err.contains("$stdDevPop") && err.contains("$minN") && err.contains("$count"), "{err}");
}
