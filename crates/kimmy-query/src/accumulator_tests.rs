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
    let mut state = spec.init();
    let docs = xs(&[1, 5, 9, 20]);
    for d in &docs {
        state.accumulate(&spec, &crate::expr::Scope::new(d)).unwrap();
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
fn ties_go_to_the_first_arrival_for_top_and_for_bottom() {
    // b and d tie at 30. The reference answers the first of them, b, for $top of
    // the descending order and for $bottom of the ascending one.
    assert_eq!(
        one(doc! {"$top": {"sortBy": {"score": -1}, "output": "$name"}}, scores()),
        Bson::String("b".into())
    );
    assert_eq!(
        one(doc! {"$bottom": {"sortBy": {"score": 1}, "output": "$name"}}, scores()),
        Bson::String("b".into())
    );
}

/// acc.js `top/bottom` and `topN/bottomN` (reference 6.0.5): rows b, c (tie at
/// 30), d, a, e, f by `s` descending.
fn probe_rows() -> Vec<Document> {
    [("a", 10), ("b", 30), ("c", 30), ("d", 20), ("e", 5), ("f", 1)]
        .iter()
        .map(|(n, s)| doc! {"n": *n, "s": *s})
        .collect()
}

#[test]
fn the_reference_probe_of_top_bottom_and_their_n_forms() {
    let r = group(
        doc! {
            "top": {"$top": {"sortBy": {"s": -1}, "output": "$n"}},
            "bottom": {"$bottom": {"sortBy": {"s": -1}, "output": "$n"}},
            "t": {"$topN": {"n": 3, "sortBy": {"s": -1}, "output": "$n"}},
            "b": {"$bottomN": {"n": 3, "sortBy": {"s": -1}, "output": "$n"}},
        },
        probe_rows(),
    );
    assert_eq!(
        r,
        doc! {"_id": null, "top": "b", "bottom": "f", "t": ["b", "c", "d"], "b": ["a", "e", "f"]}
    );
}

/// extra.js `bottom ties`, `bottomN 2 ties`, `topN 2 ties desc` and
/// `top ties, reverse arrival`.
#[test]
fn ties_in_the_reference_cases() {
    let rows = vec![
        doc! {"_id": 1, "s": 1, "n": "a"},
        doc! {"_id": 2, "s": 1, "n": "b"},
        doc! {"_id": 3, "s": 1, "n": "c"},
        doc! {"_id": 4, "s": 0, "n": "d"},
    ];
    let bottom = |acc: Document, rows: Vec<Document>| one(acc, rows);
    assert_eq!(
        bottom(doc! {"$bottom": {"sortBy": {"s": 1}, "output": "$n"}}, rows.clone()),
        Bson::String("a".into())
    );
    assert_eq!(
        bottom(doc! {"$bottomN": {"n": 2, "sortBy": {"s": 1}, "output": "$n"}}, rows.clone()),
        Bson::Array(vec!["a".into(), "b".into()])
    );
    assert_eq!(
        bottom(doc! {"$topN": {"n": 2, "sortBy": {"s": -1}, "output": "$n"}}, rows.clone()),
        Bson::Array(vec!["a".into(), "b".into()])
    );
    let mut reversed = rows;
    reversed.reverse();
    assert_eq!(
        bottom(doc! {"$top": {"sortBy": {"s": -1}, "output": "$n"}}, reversed),
        Bson::String("c".into())
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

/// acc.js `array key asc` / `array key desc`, extra.js `top arrays asc` /
/// `top arrays desc` / `bottom arrays asc` (reference 6.0.5): the whole array is
/// the key, compared element by element.
#[test]
fn an_array_key_is_compared_as_a_whole() {
    let data = vec![doc! {"v": [1, 9], "i": 1}, doc! {"v": [5, 6], "i": 2}];
    let asc = one(doc! {"$top": {"sortBy": {"v": 1}, "output": "$i"}}, data.clone());
    let desc = one(doc! {"$top": {"sortBy": {"v": -1}, "output": "$i"}}, data);
    assert_eq!((asc, desc), (Bson::Int32(1), Bson::Int32(2)));
    let data = vec![doc! {"v": [3, 1], "i": 1}, doc! {"v": [2, 9], "i": 2}];
    let r = group(
        doc! {
            "ta": {"$top": {"sortBy": {"v": 1}, "output": "$i"}},
            "td": {"$top": {"sortBy": {"v": -1}, "output": "$i"}},
            "ba": {"$bottom": {"sortBy": {"v": 1}, "output": "$i"}},
        },
        data,
    );
    assert_eq!(r, doc! {"_id": null, "ta": 2, "td": 1, "ba": 1});
}

/// extra2.js `topN asc order` / `topN desc order` / `bottomN asc order`: eleven
/// shapes of one key. Ascending `[] < null = missing < 4 < 'str' < [1,10] < [2] <
/// [2,9] < [3,1] < [5] < [[0]]`, descending the reverse, ties in arrival order.
fn eleven_shapes() -> Vec<Document> {
    vec![
        doc! {"_id": "A", "v": [3, 1]},
        doc! {"_id": "B", "v": [2, 9]},
        doc! {"_id": "C", "v": [5]},
        doc! {"_id": "D", "v": 4},
        doc! {"_id": "E", "v": Vec::<Bson>::new()},
        doc! {"_id": "F", "v": Bson::Null},
        doc! {"_id": "G", "v": [1, 10]},
        doc! {"_id": "H", "v": [2]},
        doc! {"_id": "I", "v": [[0]]},
        doc! {"_id": "J", "v": "str"},
        doc! {"_id": "K"},
    ]
}

fn ids(v: &[&str]) -> Bson {
    Bson::Array(v.iter().map(|s| Bson::String((*s).into())).collect())
}

#[test]
fn eleven_shapes_of_an_array_key_order_as_the_reference_orders_them() {
    let top = |dir: i32, name: &str| {
        one(doc! {name: {"n": 20, "sortBy": {"v": dir}, "output": "$_id"}}, eleven_shapes())
    };
    let asc = ids(&["E", "F", "K", "D", "J", "G", "H", "B", "A", "C", "I"]);
    assert_eq!(top(1, "$topN"), asc);
    assert_eq!(top(-1, "$topN"), ids(&["I", "C", "A", "B", "H", "G", "J", "D", "F", "K", "E"]));
    assert_eq!(top(1, "$bottomN"), asc);
}

/// extra2.js `top two keys`: `sortBy {v: 1, _id: -1}`, `n: 3`.
#[test]
fn a_second_key_breaks_ties_of_the_first() {
    let r = one(
        doc! {"$topN": {"n": 3, "sortBy": {"v": 1, "_id": -1}, "output": "$_id"}},
        eleven_shapes(),
    );
    assert_eq!(r, ids(&["E", "K", "F"]));
}

/// extra.js `topN empty array/null/missing`: MinKey sorts below an empty array.
#[test]
fn an_empty_array_sorts_between_min_key_and_null() {
    let data = vec![
        doc! {"_id": 1, "v": Vec::<Bson>::new()},
        doc! {"_id": 2, "v": Bson::Null},
        doc! {"_id": 3},
        doc! {"_id": 4, "v": [Bson::Null]},
        doc! {"_id": 5, "v": Bson::MinKey},
    ];
    let asc = one(doc! {"$topN": {"n": 5, "sortBy": {"v": 1}, "output": "$_id"}}, data.clone());
    let desc = one(doc! {"$topN": {"n": 5, "sortBy": {"v": -1}, "output": "$_id"}}, data);
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|i| Bson::Int32(*i)).collect());
    assert_eq!(asc, ints(&[5, 1, 2, 3, 4]));
    assert_eq!(desc, ints(&[4, 2, 3, 1, 5]));
}

/// extra2.js `nested asc` / `nested desc` and extra.js `top nested array path`:
/// a path through an array of documents is the array of what each holds.
#[test]
fn a_path_through_an_array_of_documents_is_the_array_of_its_values() {
    let data = vec![
        doc! {"_id": 1, "a": {"b": [4, 0]}},
        doc! {"_id": 2, "a": [{"b": 3}, {"b": 1}]},
        doc! {"_id": 3, "a": [{"b": [2]}]},
    ];
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|i| Bson::Int32(*i)).collect());
    let asc = one(doc! {"$topN": {"n": 5, "sortBy": {"a.b": 1}, "output": "$_id"}}, data.clone());
    let desc = one(doc! {"$topN": {"n": 5, "sortBy": {"a.b": -1}, "output": "$_id"}}, data);
    assert_eq!(asc, ints(&[2, 1, 3]));
    assert_eq!(desc, ints(&[3, 1, 2]));
    let data = vec![doc! {"_id": 1, "a": [{"b": 5}, {"b": 1}]}, doc! {"_id": 2, "a": [{"b": 3}]}];
    let r = group(
        doc! {
            "asc": {"$top": {"sortBy": {"a.b": 1}, "output": "$_id"}},
            "desc": {"$top": {"sortBy": {"a.b": -1}, "output": "$_id"}},
        },
        data,
    );
    assert_eq!(r, doc! {"_id": null, "asc": 2, "desc": 1});
}

/// `sortBy: {}` (extra.js `topN empty sortBy`): every row ties, so rows stay in
/// arrival order.
#[test]
fn an_empty_sort_by_answers_in_arrival_order() {
    assert_eq!(
        one(doc! {"$topN": {"n": 2, "sortBy": {}, "output": "$name"}}, scores()),
        Bson::Array(vec!["a".into(), "b".into()])
    );
    assert_eq!(
        one(doc! {"$top": {"sortBy": {}, "output": "$name"}}, scores()),
        Bson::String("a".into())
    );
    assert_eq!(
        one(doc! {"$bottomN": {"n": 3, "sortBy": {}, "output": "$name"}}, scores()),
        Bson::Array(vec!["a".into(), "b".into(), "c".into()])
    );
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
        Bson::Array(vec!["b".into()])
    );
}

#[test]
fn top_and_bottom_argument_errors() {
    assert!(refused(doc! {"$top": {"output": "$x"}}).contains("`sortBy`"));
    assert!(refused(doc! {"$top": {"sortBy": {"x": 1}}}).contains("`output`"));
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

/// extra.js `firstN n 1e6 on 6 docs` and `topN n max long on 6 docs`: an `n`
/// past anything the group can hold answers every row, whatever the pipeline
/// limit is.
#[test]
fn a_huge_n_answers_every_row_of_a_small_group() {
    let limits = Limits { max_documents: 10 };
    let data: Vec<Document> = (1..=6).map(|i| doc! {"_id": i, "x": i}).collect();
    let all: Vec<i32> = (1..=6).collect();
    let ints = |v: &[i32]| Bson::Array(v.iter().map(|i| Bson::Int32(*i)).collect());
    let rev: Vec<i32> = all.iter().rev().copied().collect();
    for (acc, want) in [
        (doc! {"$firstN": {"n": 1_000_000, "input": "$x"}}, ints(&all)),
        (doc! {"$lastN": {"n": i64::MAX, "input": "$x"}}, ints(&all)),
        (doc! {"$maxN": {"n": 1_000_000, "input": "$x"}}, ints(&rev)),
        (doc! {"$minN": {"n": i64::MAX, "input": "$x"}}, ints(&all)),
        (doc! {"$topN": {"n": 1_000_000, "sortBy": {"x": 1}, "output": "$x"}}, ints(&all)),
        (doc! {"$bottomN": {"n": i64::MAX, "sortBy": {"x": -1}, "output": "$x"}}, ints(&rev)),
    ] {
        let out = run_with(doc! {"$group": {"_id": null, "r": acc.clone()}}, data.clone(), &limits)
            .unwrap_or_else(|e| panic!("{acc}: {e}"));
        assert_eq!(out[0].get("r"), Some(&want), "{acc}");
    }
}

/// The buffers are bounded by the stage's input, as `$push`'s are: many groups
/// each holding up to `n` rows are not refused, whatever the pipeline limit
/// says about the rows held in all.
#[test]
fn buffers_across_many_groups_are_not_refused() {
    let limits = Limits { max_documents: 10 };
    let input: Vec<Document> = (0..12).map(|i| doc! {"g": i % 3, "x": i}).collect();
    for acc in [
        doc! {"$topN": {"n": 10, "sortBy": {"x": 1}, "output": "$x"}},
        doc! {"$bottomN": {"n": 10, "sortBy": {"x": 1}, "output": "$x"}},
        doc! {"$maxN": {"n": 10, "input": "$x"}},
        doc! {"$minN": {"n": 10, "input": "$x"}},
        doc! {"$firstN": {"n": 10, "input": "$x"}},
        doc! {"$lastN": {"n": 10, "input": "$x"}},
    ] {
        let out =
            run_with(doc! {"$group": {"_id": "$g", "r": acc.clone()}}, input.clone(), &limits)
                .unwrap_or_else(|e| panic!("{acc}: {e}"));
        assert_eq!(out.len(), 3, "{acc}");
        for group in &out {
            assert_eq!(group.get_array("r").unwrap().len(), 4, "{acc}");
        }
    }
    // Two of them beside a $push, in a $bucketAuto too.
    let out = run_with(
        doc! {"$bucketAuto": {"groupBy": "$x", "buckets": 2, "output": {
        "a": {"$topN": {"n": 10, "sortBy": {"x": 1}, "output": "$x"}},
        "b": {"$maxN": {"n": 10, "input": "$x"}},
        "c": {"$push": "$x"}}}},
        input,
        &limits,
    )
    .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get_array("a").unwrap().len(), 6);
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

// ---------------------------------------------------------------------------
// The buffers are not quadratic
// ---------------------------------------------------------------------------

#[test]
fn ranked_buffers_take_sixty_thousand_rows_with_n_of_sixty_thousand() {
    // A sorted-vector insert is O(n) per row and takes seconds here; a heap does
    // not. Reverse order is the worst case for an insert at the front.
    let n = 60_000;
    let data: Vec<Document> = (0..n).rev().map(|i| doc! {"x": i}).collect();
    let limits = Limits { max_documents: 200_000 };
    for (acc, ascending) in [
        (doc! {"$topN": {"n": n, "sortBy": {"x": 1}, "output": "$x"}}, true),
        (doc! {"$bottomN": {"n": n, "sortBy": {"x": 1}, "output": "$x"}}, true),
        (doc! {"$minN": {"n": n, "input": "$x"}}, true),
        (doc! {"$maxN": {"n": n, "input": "$x"}}, false),
    ] {
        let out = run_with(doc! {"$group": {"_id": null, "r": acc.clone()}}, data.clone(), &limits)
            .unwrap();
        let r = out[0].get_array("r").unwrap();
        assert_eq!(r.len(), n as usize, "{acc}");
        let first = if ascending { 0 } else { n - 1 };
        assert_eq!(r[0], Bson::Int32(first), "{acc}");
        assert_eq!(r[n as usize - 1], Bson::Int32(n - 1 - first), "{acc}");
    }
}

#[test]
fn equal_values_of_different_numeric_types_keep_their_arrival_order() {
    let data = vec![doc! {"x": 4}, doc! {"x": 4.0}, doc! {"x": 4i64}, doc! {"x": 1}];
    assert_eq!(
        one(doc! {"$maxN": {"n": 2, "input": "$x"}}, data.clone()),
        Bson::Array(vec![Bson::Int32(4), Bson::Double(4.0)])
    );
    assert_eq!(
        one(doc! {"$minN": {"n": 3, "input": "$x"}}, data),
        Bson::Array(vec![Bson::Int32(1), Bson::Int32(4), Bson::Double(4.0)])
    );
}

// ---------------------------------------------------------------------------
// The array forms: $firstN, $lastN, $maxN, $minN, $stdDevPop, $stdDevSamp as expressions
// ---------------------------------------------------------------------------

fn project_one(expr: Bson, input: Document) -> kimmy_core::Result<Option<Bson>> {
    let out = run_with(doc! {"$project": {"_id": 0, "r": expr}}, vec![input], &Limits::default())?;
    Ok(out[0].get("r").cloned())
}

fn expr_result(expr: Bson) -> Bson {
    project_one(expr, doc! {}).unwrap().expect("a value")
}

#[test]
fn the_n_operators_work_on_an_array_expression() {
    // extra.js `firstN as expression` and `maxN as expression` (reference 6.0.5).
    assert_eq!(
        expr_result(bson::bson!({"$firstN": {"n": 2, "input": [1, 2, 3]}})),
        Bson::Array(vec![1.into(), 2.into()])
    );
    assert_eq!(
        expr_result(bson::bson!({"$maxN": {"n": 2, "input": [1, 3, 2, null]}})),
        Bson::Array(vec![3.into(), 2.into()])
    );
    // The siblings answer what the accumulators answer over the same values.
    assert_eq!(
        expr_result(bson::bson!({"$lastN": {"n": 2, "input": [1, null, 3]}})),
        Bson::Array(vec![Bson::Null, 3.into()])
    );
    assert_eq!(
        expr_result(bson::bson!({"$firstN": {"n": 2, "input": [null, 3]}})),
        Bson::Array(vec![Bson::Null, 3.into()])
    );
    assert_eq!(
        expr_result(bson::bson!({"$minN": {"n": 5, "input": [4, null, 1, "a", 2]}})),
        Bson::Array(vec![1.into(), 2.into(), 4.into(), "a".into()])
    );
}

#[test]
fn the_n_operators_read_an_array_field_and_n_per_document() {
    let input = doc! {"xs": [5, 1, 9], "k": 2};
    assert_eq!(
        project_one(bson::bson!({"$maxN": {"n": "$k", "input": "$xs"}}), input.clone()).unwrap(),
        Some(Bson::Array(vec![9.into(), 5.into()]))
    );
    assert_eq!(
        project_one(bson::bson!({"$lastN": {"n": {"$add": ["$k", 1]}, "input": "$xs"}}), input)
            .unwrap(),
        Some(Bson::Array(vec![5.into(), 1.into(), 9.into()]))
    );
}

#[test]
fn the_n_operators_on_an_array_refuse_a_bad_n_or_input() {
    for (expr, text) in [
        (bson::bson!({"$firstN": {"n": 0, "input": [1]}}), "greater than 0"),
        (bson::bson!({"$firstN": {"n": 1.5, "input": [1]}}), "must be an integer"),
        (bson::bson!({"$maxN": {"n": "$missing", "input": [1]}}), "must be an integer"),
        (bson::bson!({"$maxN": {"n": 1, "input": "text"}}), "to be an array"),
        (bson::bson!({"$minN": {"input": [1]}}), "`n`"),
        (bson::bson!({"$minN": {"n": 1}}), "`input`"),
        (bson::bson!({"$lastN": {"n": 1, "input": [1], "z": 1}}), "does not take `z`"),
    ] {
        let err = project_one(expr.clone(), doc! {}).expect_err("refused").to_string();
        assert!(err.contains(text), "{expr}: {err}");
    }
}

#[test]
fn the_n_operators_on_a_null_or_missing_input_answer_null() {
    assert_eq!(
        project_one(bson::bson!({"$firstN": {"n": 1, "input": "$nothing"}}), doc! {}).unwrap(),
        Some(Bson::Null)
    );
    assert_eq!(
        project_one(bson::bson!({"$maxN": {"n": 1, "input": null}}), doc! {}).unwrap(),
        Some(Bson::Null)
    );
}

#[test]
fn std_dev_works_on_an_array_expression() {
    // extra.js `stdDevPop in $project (array)` and `stdDevSamp in $project (args)`.
    assert_eq!(
        expr_result(bson::bson!({"$stdDevPop": [1, 2, 3, 4]})),
        Bson::Double(1.118033988749895)
    );
    assert_eq!(
        expr_result(bson::bson!({"$stdDevSamp": [1.0, 2, "x", null]})),
        Bson::Double(std::f64::consts::FRAC_1_SQRT_2) // 0.7071067811865476
    );
    // One operand that is an array is its elements.
    assert_eq!(
        project_one(bson::bson!({"$stdDevPop": "$xs"}), doc! {"xs": [1, 2, 3, 4]}).unwrap(),
        Some(Bson::Double(1.118033988749895))
    );
    assert_eq!(
        expr_result(bson::bson!({"$stdDevPop": [[1, 2, 3, 4]]})),
        Bson::Double(1.118033988749895)
    );
}

#[test]
fn std_dev_on_an_array_with_too_few_numbers() {
    assert_eq!(expr_result(bson::bson!({"$stdDevPop": [7]})), Bson::Double(0.0));
    assert_eq!(expr_result(bson::bson!({"$stdDevSamp": [7]})), Bson::Null);
    assert_eq!(expr_result(bson::bson!({"$stdDevPop": ["a", null]})), Bson::Null);
    assert_eq!(expr_result(bson::bson!({"$stdDevSamp": []})), Bson::Null);
}

#[test]
fn the_group_accumulators_keep_working_beside_the_expression_forms() {
    // `$firstN` in `$group` is the accumulator, in `$project` the operator.
    let out = run_with(
        doc! {"$group": {"_id": null, "r": {"$firstN": {"n": 1, "input": "$x"}}}},
        xs(&[3, 4]),
        &Limits::default(),
    )
    .unwrap();
    assert_eq!(out[0].get_array("r").unwrap(), &vec![Bson::Int32(3)]);
}

// ---------------------------------------------------------------------------
// The reference probe (acc.js, answers in reference-6.0.5/acc.out)
// ---------------------------------------------------------------------------

fn probe() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "g": "a", "x": 2, "s": 10, "n": "a"},
        doc! {"_id": 2, "g": "a", "x": 4, "s": 30, "n": "b"},
        doc! {"_id": 3, "g": "a", "x": 4, "s": 30, "n": "c"},
        doc! {"_id": 4, "g": "a", "x": Bson::Null, "s": 20, "n": "d"},
        doc! {"_id": 5, "g": "a", "s": 5, "n": "e"},
        doc! {"_id": 6, "g": "a", "x": "str", "s": 1, "n": "f"},
        doc! {"_id": 7, "g": "b", "x": 7},
        doc! {"_id": 8, "g": "c", "x": "only text"},
    ]
}

fn probe_group_a() -> Vec<Document> {
    probe().into_iter().filter(|d| d.get_str("g").is_ok_and(|g| g == "a")).collect()
}

#[test]
fn probe_std_dev_and_count_per_group() {
    let out = run_with(
        doc! {"$group": {"_id": "$g",
        "pop": {"$stdDevPop": "$x"}, "samp": {"$stdDevSamp": "$x"}, "c": {"$count": {}}}},
        probe(),
        &Limits::default(),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": "a", "pop": 0.9428090415820634, "samp": 1.1547005383792515, "c": 6},
            doc! {"_id": "b", "pop": 0.0, "samp": Bson::Null, "c": 1},
            doc! {"_id": "c", "pop": Bson::Null, "samp": Bson::Null, "c": 1},
        ]
    );
}

#[test]
fn probe_the_n_forms() {
    let r = group(
        doc! {
            "big": {"$topN": {"n": 50, "sortBy": {"s": 1}, "output": "$s"}},
            "missing": {"$topN": {"n": 6, "sortBy": {"_id": 1}, "output": "$x"}},
            "f": {"$firstN": {"n": 4, "input": "$x"}},
            "l": {"$lastN": {"n": 4, "input": "$x"}},
            "mx": {"$maxN": {"n": 3, "input": "$x"}},
            "mn": {"$minN": {"n": 3, "input": "$x"}},
            "two": {"$firstN": {"n": 2.0, "input": "$x"}},
        },
        probe_group_a(),
    );
    assert_eq!(
        r,
        doc! {
            "_id": null,
            "big": [1, 5, 10, 20, 30, 30],
            "missing": [2, 4, 4, null, null, "str"],
            "f": [2, 4, 4, null],
            "l": [4, null, null, "str"],
            "mx": ["str", 4, 4],
            "mn": [2, 4, 4],
            "two": [2, 4],
        }
    );
    // Nothing but a null: `$maxN` answers [].
    let only_null = vec![probe().remove(3)];
    assert_eq!(one(doc! {"$maxN": {"n": 2, "input": "$x"}}, only_null), Bson::Array(vec![]));
}

#[test]
fn probe_n_refusals_and_a_field_for_n() {
    for n in [Bson::Int32(0), Bson::Int32(-1), Bson::Double(2.5), "x".into(), Bson::Null] {
        let acc = doc! {"$firstN": {"n": n.clone(), "input": "$x"}};
        let err = aggregate::parse(&[doc! {"$group": {"_id": null, "r": acc}}]);
        let outcome = err.map(|stages| aggregate::apply(&stages[0], probe(), &Limits::default()));
        assert!(matches!(outcome, Err(_) | Ok(Err(_))), "n = {n:?} must be refused");
    }
    // `n: '$s'` is missing for the group (error 5787902 in the reference).
    let outcome = aggregate::parse(&[doc! {"$group": {"_id": null,
    "r": {"$firstN": {"n": "$s", "input": "$x"}}}}])
    .map(|stages| aggregate::apply(&stages[0], probe(), &Limits::default()));
    assert!(matches!(outcome, Err(_) | Ok(Err(_))));
}

#[test]
fn probe_stability_decimal_and_big_longs() {
    let big: Vec<Document> = [4.0, 7.0, 13.0, 16.0].iter().map(|d| doc! {"x": 1e9 + d}).collect();
    let r = group(doc! {"pop": {"$stdDevPop": "$x"}, "samp": {"$stdDevSamp": "$x"}}, big);
    assert_eq!(r, doc! {"_id": null, "pop": 4.743416490252569, "samp": 5.477225575051661});
    let d = |s: &str| Bson::Decimal128(s.parse::<Decimal128>().unwrap());
    let r = group(
        doc! {"pop": {"$stdDevPop": "$x"}},
        vec![doc! {"x": d("1.5")}, doc! {"x": d("2.5")}, doc! {"x": 3.5}],
    );
    assert_eq!(r, doc! {"_id": null, "pop": 0.816496580927726});
    // extra.js `stdDev big longs`: 2.0; `stdDev with Infinity (and NaN)`: NaN.
    let longs = vec![doc! {"x": 9_007_199_254_740_993_i64}, doc! {"x": 9_007_199_254_740_995_i64}];
    assert_eq!(one(doc! {"$stdDevPop": "$x"}, longs), Bson::Double(2.0));
    let inf = vec![doc! {"x": 1}, doc! {"x": f64::INFINITY}];
    assert!(f(&one(doc! {"$stdDevPop": "$x"}, inf.clone())).is_nan());
    assert!(f(&one(doc! {"$stdDevSamp": "$x"}, inf)).is_nan());
}

/// extra.js `maxN undefined`: a stored undefined is skipped by `$maxN`.
#[test]
fn max_n_and_min_n_skip_undefined() {
    let data = vec![doc! {"x": Bson::Undefined}, doc! {"x": 5}, doc! {"y": 1}];
    assert_eq!(
        one(doc! {"$maxN": {"n": 3, "input": "$x"}}, data.clone()),
        Bson::Array(vec![5.into()])
    );
    assert_eq!(one(doc! {"$minN": {"n": 3, "input": "$x"}}, data), Bson::Array(vec![5.into()]));
}
