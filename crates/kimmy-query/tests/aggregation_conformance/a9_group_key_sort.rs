//! A9: a sort whose only key is `_id` (`{_id: 1}` or `{_id: -1}`) compares the `_id` values
//! whole, wherever the `_id` came from and with any stages in between (q7s4f:31 to 48,
//! q7s4a:65, 277, 303, and the 147 cases of q7s4j). Every other sort, `{_id: 1, n: 1}` included,
//! reads an array by its least or greatest element.

use super::common::{json, run};
use bson::{Bson, Document, bson, doc};
use kimmy_core::Error;
use kimmy_query::aggregate::{self, Limits, Stage};
use kimmy_query::budget::Lease;
use kimmy_query::pipeline;

/// The 14 documents of q7s4f:31, each with a `v`: arrays, scalars, a document, null and none.
fn values() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "v": [1] },
        doc! { "_id": 2, "v": [3, 1] },
        doc! { "_id": 3, "v": [] },
        doc! { "_id": 4, "v": [[2]] },
        doc! { "_id": 5, "v": ["a"] },
        doc! { "_id": 6, "v": [5] },
        doc! { "_id": 7, "v": [0, 9] },
        doc! { "_id": 8, "v": 0 },
        doc! { "_id": 9, "v": 2 },
        doc! { "_id": 10, "v": "b" },
        doc! { "_id": 11, "v": Bson::Null },
        doc! { "_id": 12 },
        doc! { "_id": 13, "v": { "a": 1 } },
        doc! { "_id": 14, "v": [1, 2] },
    ]
}

/// `null < 0 < 2 < "b" < {a: 1} < [] < [0, 9] < [1] < [1, 2] < [3, 1] < [5] < ["a"] < [[2]]`
/// (q7s4f:31), with the count of each: the null and the missing `v` are one group of two.
fn ascending() -> Vec<(Bson, i32)> {
    vec![
        (Bson::Null, 2),
        (bson!(0), 1),
        (bson!(2), 1),
        (bson!("b"), 1),
        (bson!({ "a": 1 }), 1),
        (bson!([]), 1),
        (bson!([0, 9]), 1),
        (bson!([1]), 1),
        (bson!([1, 2]), 1),
        (bson!([3, 1]), 1),
        (bson!([5]), 1),
        (bson!(["a"]), 1),
        (bson!([[2]]), 1),
    ]
}

fn ids(out: &[Document]) -> Vec<Bson> {
    out.iter().map(|d| d.get("_id").cloned().unwrap_or(Bson::Null)).collect()
}

fn keys(rows: &[(Bson, i32)]) -> Vec<Bson> {
    rows.iter().map(|(k, _)| k.clone()).collect()
}

#[test]
fn group_keys_sort_whole_ascending_and_descending() {
    // f:31 and f:40 (the exact reverse)
    for (direction, reverse) in [(1, false), (-1, true)] {
        let out = run(
            &[
                doc! { "$group": { "_id": "$v", "c": { "$sum": 1 } } },
                doc! { "$sort": { "_id": direction } },
            ],
            values(),
        )
        .unwrap();
        let mut want = ascending();
        if reverse {
            want.reverse();
        }
        assert_eq!(ids(&out), keys(&want), "direction {direction}");
        let counts: Vec<i32> = out.iter().map(|d| d.get_i32("c").unwrap()).collect();
        assert_eq!(counts, want.iter().map(|(_, c)| *c).collect::<Vec<_>>());
    }
}

#[test]
fn a_sort_by_count_then_the_id_sort_is_whole_too() {
    // f:38 and f:47
    for (direction, reverse) in [(1, false), (-1, true)] {
        let out =
            run(&[doc! { "$sortByCount": "$v" }, doc! { "$sort": { "_id": direction } }], values())
                .unwrap();
        let mut want = keys(&ascending());
        if reverse {
            want.reverse();
        }
        assert_eq!(ids(&out), want, "direction {direction}");
    }
}

#[test]
fn a_sort_before_the_group_and_an_unwind_before_it_change_nothing() {
    // a:65 (`$sort` first), a:251 and a:335 (`$unwind` first): the rule is the stage's.
    let out = run(
        &[
            doc! { "$sort": { "_id": 1 } },
            doc! { "$group": { "_id": "$g" } },
            doc! { "$sort": { "_id": 1 } },
        ],
        vec![
            doc! { "_id": 1, "g": "a" },
            doc! { "_id": 2, "g": [1] },
            doc! { "_id": 3, "g": Bson::Null },
            doc! { "_id": 4, "g": "c" },
            doc! { "_id": 5, "g": "b" },
        ],
    )
    .unwrap();
    assert_eq!(ids(&out), vec![Bson::Null, "a".into(), "b".into(), "c".into(), bson!([1])]);
}

#[test]
fn a_limit_after_the_sort_takes_the_first_of_the_whole_order() {
    // f:37 and f:46, through the stages one at a time and through the chain that keeps a heap.
    for (direction, want) in [
        (1, vec![Bson::Null, bson!(0), bson!(2), bson!("b")]),
        (-1, vec![bson!([[2]]), bson!(["a"]), bson!([5]), bson!([3, 1])]),
    ] {
        let pipeline = [
            doc! { "$group": { "_id": "$v" } },
            doc! { "$sort": { "_id": direction } },
            doc! { "$limit": 4 },
        ];
        let out = run(&pipeline, values()).unwrap();
        assert_eq!(ids(&out), want, "stage by stage, direction {direction}");

        let stages = aggregate::parse(&pipeline).unwrap();
        let limits = Limits::default();
        let runner = |stage: &Stage, input: Vec<Document>, _: Option<Lease>| {
            aggregate::apply(stage, input, &limits)
        };
        let mut chain: pipeline::Chain<'_, Error> = pipeline::build(&stages, &runner, &limits, &[]);
        let _flow = chain.push_all(values()).unwrap();
        assert_eq!(ids(&chain.finish().unwrap()), want, "the chain, direction {direction}");
    }
}

#[test]
fn a_facet_branch_sorts_its_group_keys_whole() {
    // a:303
    let input = vec![
        doc! { "_id": 1, "g": "a" },
        doc! { "_id": 2, "g": [1] },
        doc! { "_id": 3, "g": Bson::Null },
        doc! { "_id": 4, "g": "b" },
    ];
    let out = run(
        &[doc! { "$facet": { "b": [
            { "$group": { "_id": "$g", "c": { "$sum": 1 } } },
            { "$sort": { "_id": 1 } },
        ] } }],
        input,
    )
    .unwrap();
    let branch = out[0].get_array("b").unwrap();
    let got: Vec<Bson> = branch.iter().map(|d| d.as_document().unwrap()["_id"].clone()).collect();
    assert_eq!(got, vec![Bson::Null, "a".into(), "b".into(), bson!([1])]);
}

/// A recorded case: its line in `q7s4j.jsonl`, its pipeline and the `_id` of each row in order.
type Row = (u32, Vec<Document>, Vec<Bson>);

fn stages_of(stages: Bson) -> Vec<Document> {
    stages.as_array().unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect()
}

fn list(values: Bson) -> Vec<Bson> {
    values.as_array().unwrap().clone()
}

#[test]
fn the_sixteen_document_shapes_sort_whole_after_each_grouping_stage() {
    // q7s4j 1 to 8: `$group` by a document, by `{k}` and by `{k, j}`, and `$sortByCount`,
    // ascending and descending. The order is C7's: value type, then name, then value.
    let mut failures = Vec::new();
    for (line, stages, want) in document_rows() {
        match run(&stages, shapes20()) {
            Ok(out) if ids(&out) == want => {}
            Ok(out) => failures
                .push(format!("q7s4j:{line}\n   ours     {:?}\n   recorded {want:?}", ids(&out))),
            Err(e) => failures.push(format!("q7s4j:{line}: {e}")),
        }
    }
    assert!(failures.is_empty(), "{} rows differ:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn documents_beside_arrays_and_every_other_source_of_the_id_sort_whole() {
    // q7s4j 17 and 21 (`$group`), 61 to 68 (`$replaceWith`, `$project`, `$addFields`, `$set`).
    let mut failures = Vec::new();
    for (line, stages, want) in mixed_rows() {
        match run(&stages, shapes26()) {
            Ok(out) if ids(&out) == want => {}
            Ok(out) => failures
                .push(format!("q7s4j:{line}\n   ours     {:?}\n   recorded {want:?}", ids(&out))),
            Err(e) => failures.push(format!("q7s4j:{line}: {e}")),
        }
    }
    assert!(failures.is_empty(), "{} rows differ:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn stages_between_the_source_of_the_id_and_the_sort_change_nothing() {
    // q7s4j 107 to 111: `$match`, `$limit`, `$skip`, `$addFields`, `$project`.
    let (_, _, want) = mixed_rows().into_iter().find(|(line, ..)| *line == 61).unwrap();
    let between = [
        doc! { "$match": {} },
        doc! { "$limit": 1000 },
        doc! { "$skip": 0 },
        doc! { "$addFields": { "z": 1 } },
        doc! { "$project": { "_id": 1, "n": 1 } },
    ];
    for stage in between {
        let out = run(
            &[
                doc! { "$replaceWith": { "_id": "$v", "n": "$_id" } },
                stage.clone(),
                doc! { "$sort": { "_id": 1 } },
            ],
            shapes26(),
        )
        .unwrap();
        assert_eq!(ids(&out), want, "{stage}");
    }
}

#[test]
fn a_sort_with_a_second_key_or_a_sub_path_reads_the_least_element() {
    // q7s4j:99 `{_id: 1, n: 1}` over `$replaceWith`; q7s4j:43 `{"_id.a": 1, _id: 1}`.
    let out = run(
        &[
            doc! { "$replaceWith": { "_id": "$v", "x": "$v", "n": "$_id" } },
            doc! { "$sort": { "_id": 1, "n": 1 } },
        ],
        shapes26(),
    )
    .unwrap();
    assert_eq!(ids(&out), two_key_ids());
    let out = run(
        &[
            doc! { "$group": { "_id": "$v", "c": { "$sum": 1 } } },
            doc! { "$sort": { "_id.a": 1, "_id": 1 } },
        ],
        shapes20(),
    )
    .unwrap();
    assert_eq!(ids(&out), sub_path_ids());
}

#[test]
fn the_bounded_sort_reads_the_whole_id_too() {
    // `$sort` then `$limit`, the heap: the first rows of q7s4j:65 and of q7s4j:61.
    let rows = mixed_rows();
    let mut failures = Vec::new();
    for (line, direction) in [(61, 1), (65, -1)] {
        let (_, _, want) = rows.iter().find(|(l, ..)| *l == line).unwrap();
        let stages = stages_of(bson!([
            { "$replaceWith": { "_id": "$v", "n": "$_id" } },
            { "$sort": { "_id": direction } },
            { "$limit": 5 },
        ]));
        let parsed = aggregate::parse(&stages).unwrap();
        let limits = Limits::default();
        let runner = |stage: &Stage, input: Vec<Document>, _: Option<Lease>| {
            aggregate::apply(stage, input, &limits)
        };
        let mut chain: pipeline::Chain<'_, Error> = pipeline::build(&parsed, &runner, &limits, &[]);
        let _flow = chain.push_all(shapes26()).unwrap();
        let got = ids(&chain.finish().unwrap());
        if got != want[..5] {
            failures
                .push(format!("q7s4j:{line}\n   ours     {got:?}\n   recorded {:?}", &want[..5]));
        }
    }
    assert!(failures.is_empty(), "{} rows differ:\n{}", failures.len(), failures.join("\n"));
}

// The recorded data, generated from q7s4j.jsonl.

fn shapes20() -> Vec<Document> {
    let rows = json(
        r#"[{"_id": 1, "v": {"a": "x"}, "w": 0}, {"_id": 2, "v": {"b": 1}, "w": 1}, {"_id": 3, "v": {"a": 2}, "w": 2}, {"_id": 4, "v": {"a": 1}, "w": 0}, {"_id": 5, "v": {"a": 1, "b": 2}, "w": 1}, {"_id": 6, "v": {"b": 2, "a": 1}, "w": 2}, {"_id": 7, "v": {}, "w": 0}, {"_id": 8, "v": {"a": {"c": 1}}, "w": 1}, {"_id": 9, "v": {"a": {"c": 1, "d": 2}}, "w": 2}, {"_id": 10, "v": {"a": {"d": 2, "c": 1}}, "w": 0}, {"_id": 11, "v": {"a": [1]}, "w": 1}, {"_id": 12, "v": {"a": null}, "w": 2}, {"_id": 13, "v": {"a": 1, "b": {"x": 1}}, "w": 0}, {"_id": 14, "v": {"a": [1, 2]}, "w": 1}, {"_id": 15, "v": {"a": []}, "w": 2}, {"_id": 16, "v": {"a": "x", "b": "y"}, "w": 0}, {"_id": 17, "v": {"a": "x"}, "w": 0}, {"_id": 18, "v": {"a": 1, "b": 2}, "w": 1}, {"_id": 19, "v": {"a": 1, "b": 2}, "w": 2}, {"_id": 20, "v": {}, "w": 0}]"#,
    );
    rows.as_array().unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect()
}

fn shapes26() -> Vec<Document> {
    let rows = json(
        r#"[{"_id": 1, "v": {"a": "x"}, "w": 0}, {"_id": 2, "v": {"b": 1}, "w": 1}, {"_id": 3, "v": {"a": 2}, "w": 2}, {"_id": 4, "v": {"a": 1}, "w": 0}, {"_id": 5, "v": {"a": 1, "b": 2}, "w": 1}, {"_id": 6, "v": {"b": 2, "a": 1}, "w": 2}, {"_id": 7, "v": {}, "w": 0}, {"_id": 8, "v": {"a": {"c": 1}}, "w": 1}, {"_id": 9, "v": {"a": {"c": 1, "d": 2}}, "w": 2}, {"_id": 10, "v": {"a": {"d": 2, "c": 1}}, "w": 0}, {"_id": 11, "v": {"a": [1]}, "w": 1}, {"_id": 12, "v": {"a": null}, "w": 2}, {"_id": 13, "v": {"a": 1, "b": {"x": 1}}, "w": 0}, {"_id": 14, "v": {"a": [1, 2]}, "w": 1}, {"_id": 15, "v": {"a": []}, "w": 2}, {"_id": 16, "v": {"a": "x", "b": "y"}, "w": 0}, {"_id": 17, "v": [1], "w": 1}, {"_id": 18, "v": [1, 2], "w": 2}, {"_id": 19, "v": [], "w": 0}, {"_id": 20, "v": [{"a": 1}], "w": 1}, {"_id": 21, "v": [[1]], "w": 2}, {"_id": 22, "v": [3, 1], "w": 0}, {"_id": 23, "v": ["a"], "w": 1}, {"_id": 24, "v": {"a": "x"}, "w": 0}, {"_id": 25, "v": {"a": 1, "b": 2}, "w": 1}, {"_id": 26, "v": {}, "w": 2}]"#,
    );
    rows.as_array().unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect()
}

/// q7s4j lines 1 to 8: the 16 document shapes (20 documents), `_id` ascending then descending.
fn document_rows() -> Vec<Row> {
    vec![
        (
            1,
            stages_of(json(
                r#"[{"$group": {"_id": "$v", "c": {"$sum": 1}}}, {"$sort": {"_id": 1}}]"#,
            )),
            list(json(
                r#"[{}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}]"#,
            )),
        ),
        (
            2,
            stages_of(json(
                r#"[{"$group": {"_id": {"k": "$v"}, "c": {"$sum": 1}}}, {"$sort": {"_id": 1}}]"#,
            )),
            list(json(
                r#"[{"k": {}}, {"k": {"a": null}}, {"k": {"a": 1}}, {"k": {"a": 1, "b": 2}}, {"k": {"a": 1, "b": {"x": 1}}}, {"k": {"a": 2}}, {"k": {"b": 1}}, {"k": {"b": 2, "a": 1}}, {"k": {"a": "x"}}, {"k": {"a": "x", "b": "y"}}, {"k": {"a": {"c": 1}}}, {"k": {"a": {"c": 1, "d": 2}}}, {"k": {"a": {"d": 2, "c": 1}}}, {"k": {"a": []}}, {"k": {"a": [1]}}, {"k": {"a": [1, 2]}}]"#,
            )),
        ),
        (
            3,
            stages_of(json(
                r#"[{"$group": {"_id": {"k": "$v", "j": "$w"}, "c": {"$sum": 1}}}, {"$sort": {"_id": 1}}]"#,
            )),
            list(json(
                r#"[{"k": {}, "j": 0}, {"k": {"a": null}, "j": 2}, {"k": {"a": 1}, "j": 0}, {"k": {"a": 1, "b": 2}, "j": 1}, {"k": {"a": 1, "b": 2}, "j": 2}, {"k": {"a": 1, "b": {"x": 1}}, "j": 0}, {"k": {"a": 2}, "j": 2}, {"k": {"b": 1}, "j": 1}, {"k": {"b": 2, "a": 1}, "j": 2}, {"k": {"a": "x"}, "j": 0}, {"k": {"a": "x", "b": "y"}, "j": 0}, {"k": {"a": {"c": 1}}, "j": 1}, {"k": {"a": {"c": 1, "d": 2}}, "j": 2}, {"k": {"a": {"d": 2, "c": 1}}, "j": 0}, {"k": {"a": []}, "j": 2}, {"k": {"a": [1]}, "j": 1}, {"k": {"a": [1, 2]}, "j": 1}]"#,
            )),
        ),
        (
            4,
            stages_of(json(r#"[{"$sortByCount": "$v"}, {"$sort": {"_id": 1}}]"#)),
            list(json(
                r#"[{}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}]"#,
            )),
        ),
        (
            5,
            stages_of(json(
                r#"[{"$group": {"_id": "$v", "c": {"$sum": 1}}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[{"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}]"#,
            )),
        ),
        (
            6,
            stages_of(json(
                r#"[{"$group": {"_id": {"k": "$v"}, "c": {"$sum": 1}}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[{"k": {"a": [1, 2]}}, {"k": {"a": [1]}}, {"k": {"a": []}}, {"k": {"a": {"d": 2, "c": 1}}}, {"k": {"a": {"c": 1, "d": 2}}}, {"k": {"a": {"c": 1}}}, {"k": {"a": "x", "b": "y"}}, {"k": {"a": "x"}}, {"k": {"b": 2, "a": 1}}, {"k": {"b": 1}}, {"k": {"a": 2}}, {"k": {"a": 1, "b": {"x": 1}}}, {"k": {"a": 1, "b": 2}}, {"k": {"a": 1}}, {"k": {"a": null}}, {"k": {}}]"#,
            )),
        ),
        (
            7,
            stages_of(json(
                r#"[{"$group": {"_id": {"k": "$v", "j": "$w"}, "c": {"$sum": 1}}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[{"k": {"a": [1, 2]}, "j": 1}, {"k": {"a": [1]}, "j": 1}, {"k": {"a": []}, "j": 2}, {"k": {"a": {"d": 2, "c": 1}}, "j": 0}, {"k": {"a": {"c": 1, "d": 2}}, "j": 2}, {"k": {"a": {"c": 1}}, "j": 1}, {"k": {"a": "x", "b": "y"}, "j": 0}, {"k": {"a": "x"}, "j": 0}, {"k": {"b": 2, "a": 1}, "j": 2}, {"k": {"b": 1}, "j": 1}, {"k": {"a": 2}, "j": 2}, {"k": {"a": 1, "b": {"x": 1}}, "j": 0}, {"k": {"a": 1, "b": 2}, "j": 2}, {"k": {"a": 1, "b": 2}, "j": 1}, {"k": {"a": 1}, "j": 0}, {"k": {"a": null}, "j": 2}, {"k": {}, "j": 0}]"#,
            )),
        ),
        (
            8,
            stages_of(json(r#"[{"$sortByCount": "$v"}, {"$sort": {"_id": -1}}]"#)),
            list(json(
                r#"[{"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}]"#,
            )),
        ),
    ]
}

/// q7s4j lines 17, 21 and 61 to 68: the same shapes beside seven arrays (26 documents).
fn mixed_rows() -> Vec<Row> {
    vec![
        (
            17,
            stages_of(json(
                r#"[{"$group": {"_id": "$v", "c": {"$sum": 1}}}, {"$sort": {"_id": 1}}]"#,
            )),
            list(json(
                r#"[{}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}, [], [1], [1, 2], [3, 1], ["a"], [{"a": 1}], [[1]]]"#,
            )),
        ),
        (
            21,
            stages_of(json(
                r#"[{"$group": {"_id": "$v", "c": {"$sum": 1}}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[[[1]], [{"a": 1}], ["a"], [3, 1], [1, 2], [1], [], {"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}]"#,
            )),
        ),
        (
            61,
            stages_of(json(
                r#"[{"$replaceWith": {"_id": "$v", "n": "$_id"}}, {"$sort": {"_id": 1}}]"#,
            )),
            list(json(
                r#"[{}, {}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}, [], [1], [1, 2], [3, 1], ["a"], [{"a": 1}], [[1]]]"#,
            )),
        ),
        (
            62,
            stages_of(json(r#"[{"$project": {"_id": "$v", "n": "$_id"}}, {"$sort": {"_id": 1}}]"#)),
            list(json(
                r#"[{}, {}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}, [], [1], [1, 2], [3, 1], ["a"], [{"a": 1}], [[1]]]"#,
            )),
        ),
        (
            63,
            stages_of(json(
                r#"[{"$addFields": {"n": "$_id", "_id": "$v"}}, {"$sort": {"_id": 1}}]"#,
            )),
            list(json(
                r#"[{}, {}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}, [], [1], [1, 2], [3, 1], ["a"], [{"a": 1}], [[1]]]"#,
            )),
        ),
        (
            64,
            stages_of(json(r#"[{"$set": {"n": "$_id", "_id": "$v"}}, {"$sort": {"_id": 1}}]"#)),
            list(json(
                r#"[{}, {}, {"a": null}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}, [], [1], [1, 2], [3, 1], ["a"], [{"a": 1}], [[1]]]"#,
            )),
        ),
        (
            65,
            stages_of(json(
                r#"[{"$replaceWith": {"_id": "$v", "n": "$_id"}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[[[1]], [{"a": 1}], ["a"], [3, 1], [1, 2], [1], [], {"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}, {}]"#,
            )),
        ),
        (
            66,
            stages_of(json(
                r#"[{"$project": {"_id": "$v", "n": "$_id"}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[[[1]], [{"a": 1}], ["a"], [3, 1], [1, 2], [1], [], {"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}, {}]"#,
            )),
        ),
        (
            67,
            stages_of(json(
                r#"[{"$addFields": {"n": "$_id", "_id": "$v"}}, {"$sort": {"_id": -1}}]"#,
            )),
            list(json(
                r#"[[[1]], [{"a": 1}], ["a"], [3, 1], [1, 2], [1], [], {"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}, {}]"#,
            )),
        ),
        (
            68,
            stages_of(json(r#"[{"$set": {"n": "$_id", "_id": "$v"}}, {"$sort": {"_id": -1}}]"#)),
            list(json(
                r#"[[[1]], [{"a": 1}], ["a"], [3, 1], [1, 2], [1], [], {"a": [1, 2]}, {"a": [1]}, {"a": []}, {"a": {"d": 2, "c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"c": 1}}, {"a": "x", "b": "y"}, {"a": "x"}, {"a": "x"}, {"b": 2, "a": 1}, {"b": 1}, {"a": 2}, {"a": 1, "b": {"x": 1}}, {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1}, {"a": null}, {}, {}]"#,
            )),
        ),
    ]
}

/// q7s4j line 99, `{_id: 1, n: 1}`: not the whole rule; the `_id` of each row in order.
fn two_key_ids() -> Vec<Bson> {
    list(json(
        r#"[[], [1], [1, 2], [3, 1], ["a"], {}, {}, {"a": null}, {"a": 1}, [{"a": 1}], {"a": 1, "b": 2}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"a": 2}, {"b": 1}, {"b": 2, "a": 1}, {"a": "x"}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}, {"a": []}, {"a": [1]}, {"a": [1, 2]}, [[1]]]"#,
    ))
}

/// q7s4j line 43, `{"_id.a": 1, _id: 1}` over the 16 shapes grouped.
fn sub_path_ids() -> Vec<Bson> {
    list(json(
        r#"[{"a": []}, {}, {"a": null}, {"b": 1}, {"a": 1}, {"a": 1, "b": 2}, {"a": 1, "b": {"x": 1}}, {"b": 2, "a": 1}, {"a": [1]}, {"a": [1, 2]}, {"a": 2}, {"a": "x"}, {"a": "x", "b": "y"}, {"a": {"c": 1}}, {"a": {"c": 1, "d": 2}}, {"a": {"d": 2, "c": 1}}]"#,
    ))
}
