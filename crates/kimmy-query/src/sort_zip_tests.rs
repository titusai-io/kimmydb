//! `$sortArray` and `$zip` (ADR-255), one row at a time: the order, the
//! lengths, what is refused, and in whose words.

use bson::{Bson, Document, bson, doc};
use kimmy_core::decimal::parse;

use crate::aggregate::{self, Limits};
use crate::expr::{Expr, MAX_ZIP_CELLS};

fn dec(text: &str) -> Bson {
    Bson::Decimal128(parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

fn eval(expr: Document, doc: Document) -> kimmy_core::Result<Bson> {
    let expr = Bson::Document(expr);
    Expr::parse(&expr)?.eval(&doc)
}

fn val(expr: Document, doc: Document) -> Bson {
    eval(expr.clone(), doc).unwrap_or_else(|e| panic!("{expr:?}: {e}"))
}

/// The message of an expression refused at parse or at evaluation.
fn err(expr: Document, doc: Document) -> String {
    eval(expr, doc).expect_err("must be refused").to_string()
}

fn sort(input: Bson, sort_by: Bson) -> Document {
    doc! { "$sortArray": { "input": input, "sortBy": sort_by } }
}

fn sorted(input: Bson, sort_by: Bson) -> Bson {
    val(sort(input, sort_by), doc! {})
}

fn zip(inputs: Bson) -> Document {
    doc! { "$zip": { "inputs": inputs } }
}

fn zip_longest(inputs: Bson, defaults: Option<Bson>) -> Document {
    let mut spec = doc! { "inputs": inputs, "useLongestLength": true };
    if let Some(defaults) = defaults {
        spec.insert("defaults", defaults);
    }
    doc! { "$zip": spec }
}

// ---------------------------------------------------------------------------
// $sortArray
// ---------------------------------------------------------------------------

#[test]
fn sort_by_one_and_minus_one_orders_scalars() {
    assert_eq!(sorted(bson!([3, 1, 2]), Bson::Int32(1)), bson!([1, 2, 3]));
    assert_eq!(sorted(bson!([3, 1, 2]), Bson::Int32(-1)), bson!([3, 2, 1]));
    assert_eq!(sorted(bson!(["b", "c", "a"]), Bson::Int64(1)), bson!(["a", "b", "c"]));
    assert_eq!(sorted(bson!(["b", "c", "a"]), Bson::Double(-1.0)), bson!(["c", "b", "a"]));
    assert_eq!(sorted(bson!(["b", "c", "a"]), dec("-1")), bson!(["c", "b", "a"]));
    assert_eq!(sorted(bson!([]), Bson::Int32(1)), bson!([]));
    assert_eq!(sorted(bson!([7]), Bson::Int32(-1)), bson!([7]));
}

#[test]
fn the_input_is_an_expression_read_in_its_scope() {
    let row = doc! { "xs": [3, 1, 2], "inner": { "ys": ["b", "a"] } };
    assert_eq!(
        val(sort(Bson::String("$xs".into()), Bson::Int32(1)), row.clone()),
        bson!([1, 2, 3])
    );
    assert_eq!(
        val(sort(Bson::String("$inner.ys".into()), Bson::Int32(1)), row.clone()),
        bson!(["a", "b"])
    );
    let computed = bson!({ "$concatArrays": ["$xs", [0]] });
    assert_eq!(val(sort(computed, Bson::Int32(-1)), row), bson!([3, 2, 1, 0]));
}

#[test]
fn mixed_types_follow_the_order_the_sort_stage_uses() {
    // null < numbers < strings < documents < arrays < booleans (ADR-238).
    let mixed = bson!([true, "a", 5, null, { "k": 1 }, [1], 2.5]);
    assert_eq!(
        sorted(mixed.clone(), Bson::Int32(1)),
        bson!([null, 2.5, 5, "a", { "k": 1 }, [1], true])
    );
    assert_eq!(sorted(mixed, Bson::Int32(-1)), bson!([true, [1], { "k": 1 }, "a", 5, 2.5, null]));
}

#[test]
fn a_decimal_sorts_among_the_numbers_by_its_exact_value() {
    let input = Bson::Array(vec![
        Bson::Double(0.1),
        dec("0.1"),
        Bson::Int32(0),
        dec("-1.5"),
        Bson::Int64(1),
        dec("1.0"),
    ]);
    let got = sorted(input, Bson::Int32(1));
    let Bson::Array(items) = got else { panic!("not an array") };
    // The double 0.1 is a different number from the decimal 0.1: it is the
    // larger, by its binary value. The decimal `1.0` and the integer `1` tie
    // and keep their input order.
    assert_eq!(items[0], dec("-1.5"));
    assert_eq!(items[1], Bson::Int32(0));
    assert_eq!(items[2], dec("0.1"));
    assert_eq!(items[3], Bson::Double(0.1));
    assert_eq!(items[4], Bson::Int64(1));
    assert_eq!(items[5], dec("1.0"));
}

#[test]
fn equal_elements_keep_their_input_order_in_both_directions() {
    // 1, 1.0 and 1_i64 are equal; their types show which is which.
    let input =
        Bson::Array(vec![Bson::Int32(1), Bson::Double(1.0), Bson::Int32(0), Bson::Int64(1)]);
    for direction in [1, -1] {
        let Bson::Array(items) = sorted(input.clone(), Bson::Int32(direction)) else {
            panic!("not an array")
        };
        let ones: Vec<&Bson> = items.iter().filter(|b| **b != Bson::Int32(0)).collect();
        assert_eq!(ones, [&Bson::Int32(1), &Bson::Double(1.0), &Bson::Int64(1)], "{direction}");
    }
}

#[test]
fn a_sort_document_orders_documents_by_its_keys() {
    let people = bson!([
        { "name": "d", "points": 5 },
        { "name": "b", "points": 9 },
        { "name": "c", "points": 5 },
        { "name": "a", "points": 9 },
    ]);
    assert_eq!(
        sorted(people.clone(), bson!({ "points": -1, "name": 1 })),
        bson!([
            { "name": "a", "points": 9 },
            { "name": "b", "points": 9 },
            { "name": "c", "points": 5 },
            { "name": "d", "points": 5 },
        ])
    );
    assert_eq!(
        sorted(people, bson!({ "name": -1 })),
        bson!([
            { "name": "d", "points": 5 },
            { "name": "c", "points": 5 },
            { "name": "b", "points": 9 },
            { "name": "a", "points": 9 },
        ])
    );
}

#[test]
fn a_sort_document_reaches_a_dotted_path_and_is_stable() {
    let rows = bson!([
        { "id": 1, "by": { "rank": 2 } },
        { "id": 2, "by": { "rank": 1 } },
        { "id": 3, "by": { "rank": 2 } },
        { "id": 4, "by": { "rank": 1 } },
    ]);
    let ids = |v: Bson| -> Vec<i32> {
        let Bson::Array(items) = v else { panic!("not an array") };
        items.iter().map(|d| d.as_document().unwrap().get_i32("id").unwrap()).collect()
    };
    assert_eq!(ids(sorted(rows.clone(), bson!({ "by.rank": 1 }))), [2, 4, 1, 3]);
    assert_eq!(ids(sorted(rows, bson!({ "by.rank": -1 }))), [1, 3, 2, 4]);
}

#[test]
fn a_missing_key_sorts_as_null_and_a_non_document_ties_with_it() {
    let input = bson!([{ "k": 2 }, 7, { "j": 1 }, { "k": 1 }, "s", { "k": null }]);
    // Null, missing and a scalar are one place, lowest; they keep input order.
    assert_eq!(
        sorted(input.clone(), bson!({ "k": 1 })),
        bson!([7, { "j": 1 }, "s", { "k": null }, { "k": 1 }, { "k": 2 }])
    );
    assert_eq!(
        sorted(input, bson!({ "k": -1 })),
        bson!([{ "k": 2 }, { "k": 1 }, 7, { "j": 1 }, "s", { "k": null }])
    );
    // A sort document over scalars alone changes nothing.
    assert_eq!(sorted(bson!([3, 1, 2]), bson!({ "k": 1 })), bson!([3, 1, 2]));
}

#[test]
fn a_number_over_documents_compares_whole_documents() {
    let docs = bson!([{ "b": 1 }, { "a": 2 }, { "a": 1, "b": 0 }, { "a": 1 }]);
    // Field by field in order, as the total order compares documents.
    assert_eq!(
        sorted(docs, Bson::Int32(1)),
        bson!([{ "a": 1 }, { "a": 1, "b": 0 }, { "a": 2 }, { "b": 1 }])
    );
}

#[test]
fn a_null_or_missing_input_gives_null_and_anything_else_is_refused() {
    assert_eq!(sorted(Bson::Null, Bson::Int32(1)), Bson::Null);
    assert_eq!(sorted(Bson::String("$gone".into()), Bson::Int32(1)), Bson::Null);
    for bad in [Bson::Int32(1), Bson::String("text".into()), bson!({ "a": 1 }), Bson::Boolean(true)]
    {
        let message = err(sort(bad.clone(), Bson::Int32(1)), doc! {});
        assert!(message.contains("$sortArray") && message.contains("array"), "{bad:?}: {message}");
    }
}

#[test]
fn a_malformed_spec_is_refused_by_name() {
    let parse = |spec: Bson| {
        Expr::parse(&Bson::Document(doc! { "$sortArray": spec }))
            .expect_err("must be refused")
            .to_string()
    };
    for (spec, want) in [
        (bson!({ "sortBy": 1 }), "needs `input`"),
        (bson!({ "input": [1] }), "needs `sortBy`"),
        (bson!({ "input": [1], "sortBy": 1, "extra": 1 }), "does not take `extra`"),
        (bson!({ "input": [1], "sortBy": 0 }), "must be 1, -1 or a sort document"),
        (bson!({ "input": [1], "sortBy": 2 }), "must be 1, -1 or a sort document"),
        (bson!({ "input": [1], "sortBy": "a" }), "found a string"),
        (bson!({ "input": [1], "sortBy": [1] }), "found an array"),
        (bson!({ "input": [1], "sortBy": null }), "found null"),
        (bson!({ "input": [1], "sortBy": {} }), "at least one key"),
        (bson!({ "input": [{ "a": 1 }], "sortBy": { "a": 0 } }), "\"a\" must be 1 or -1"),
        (bson!({ "input": [{ "a": 1 }], "sortBy": { "a": "up" } }), "\"a\" must be 1 or -1"),
        (bson!({ "input": [{ "a": 1 }], "sortBy": { "a": 1, "b": 3 } }), "\"b\" must be 1 or -1"),
        (bson!([1]), "takes a document"),
        (bson!("$xs"), "takes a document"),
    ] {
        let message = parse(spec.clone());
        assert!(message.contains("$sortArray") && message.contains(want), "{spec:?}: {message}");
    }
}

#[test]
fn variables_resolve_in_the_operands() {
    let nested = doc! { "$map": {
        "input": [[3, 1], [2, 9, 4]],
        "in": { "$sortArray": { "input": "$$this", "sortBy": -1 } },
    } };
    assert_eq!(val(nested, doc! {}), bson!([[3, 1], [9, 4, 2]]));
    let with_let = doc! { "$let": {
        "vars": { "xs": [2, 1] },
        "in": { "$sortArray": { "input": "$$xs", "sortBy": 1 } },
    } };
    assert_eq!(val(with_let, doc! {}), bson!([1, 2]));
    let unknown = doc! { "$sortArray": { "input": "$$nope", "sortBy": 1 } };
    assert!(Expr::parse(&Bson::Document(unknown)).is_err());
}

// ---------------------------------------------------------------------------
// $zip
// ---------------------------------------------------------------------------

#[test]
fn zip_pairs_by_position_to_the_shortest() {
    assert_eq!(val(zip(bson!([[1, 2, 3], ["a", "b"]])), doc! {}), bson!([[1, "a"], [2, "b"]]));
    assert_eq!(
        val(zip(bson!([[1, 2], ["a", "b", "c"], [true, false]])), doc! {}),
        bson!([[1, "a", true], [2, "b", false]])
    );
    assert_eq!(val(zip(bson!([[1, 2]])), doc! {}), bson!([[1], [2]]));
    assert_eq!(val(zip(bson!([[], [1]])), doc! {}), bson!([]));
    assert_eq!(val(zip(bson!([[], []])), doc! {}), bson!([]));
}

#[test]
fn zip_reads_its_inputs_as_expressions() {
    let row = doc! { "a": [1, 2], "b": ["x", "y"], "o": { "c": [[1], [2]] } };
    assert_eq!(val(zip(bson!(["$a", "$b"])), row.clone()), bson!([[1, "x"], [2, "y"]]));
    assert_eq!(
        val(zip(bson!(["$o.c", { "$reverseArray": "$a" }])), row),
        bson!([[[1], 2], [[2], 1]])
    );
}

#[test]
fn the_longest_length_pads_with_null_or_the_defaults() {
    let inputs = bson!([[1, 2, 3], ["a"]]);
    assert_eq!(
        val(zip_longest(inputs.clone(), None), doc! {}),
        bson!([[1, "a"], [2, null], [3, null]])
    );
    assert_eq!(
        val(zip_longest(inputs.clone(), Some(bson!([0, "z"]))), doc! {}),
        bson!([[1, "a"], [2, "z"], [3, "z"]])
    );
    // A default is only for the input that ran out: the longer one is not padded.
    assert_eq!(
        val(zip_longest(bson!([[1], [5, 6]]), Some(bson!([0, "z"]))), doc! {}),
        bson!([[1, 5], [0, 6]])
    );
    // A default is an expression, and a missing one is null.
    assert_eq!(
        val(zip_longest(bson!([[1, 2], []]), Some(bson!(["$d", "$gone"]))), doc! { "d": 9 }),
        bson!([[1, null], [2, null]])
    );
    assert_eq!(
        val(zip_longest(bson!([[], [1, 2]]), Some(bson!(["$d", 0]))), doc! { "d": 9 }),
        bson!([[9, 1], [9, 2]])
    );
    // `useLongestLength: false` is the shortest, as if it were absent.
    let explicit = doc! { "$zip": { "inputs": [[1, 2], [3]], "useLongestLength": false } };
    assert_eq!(val(explicit, doc! {}), bson!([[1, 3]]));
    // With nothing to pad, no default shows.
    assert_eq!(val(zip_longest(bson!([[1], [2]]), Some(bson!([8, 9]))), doc! {}), bson!([[1, 2]]));
    assert_eq!(val(zip_longest(bson!([[], []]), None), doc! {}), bson!([]));
}

#[test]
fn a_null_or_missing_input_makes_the_whole_result_null() {
    assert_eq!(val(zip(bson!([[1], null])), doc! {}), Bson::Null);
    assert_eq!(val(zip(bson!(["$gone", [1]])), doc! {}), Bson::Null);
    assert_eq!(
        val(zip_longest(bson!([[1, 2], "$gone"]), Some(bson!([0, 0]))), doc! {}),
        Bson::Null
    );
    // A non-array is refused even beside a null, wherever it stands.
    for inputs in [bson!([null, 5]), bson!([5, null]), bson!([[1], "text"]), bson!([{ "a": 1 }])] {
        let message = err(zip(inputs.clone()), doc! {});
        assert!(message.contains("$zip") && message.contains("array"), "{inputs:?}: {message}");
    }
}

#[test]
fn a_malformed_spec_is_refused_by_name_for_zip() {
    let parse = |spec: Bson| {
        Expr::parse(&Bson::Document(doc! { "$zip": spec }))
            .expect_err("must be refused")
            .to_string()
    };
    for (spec, want) in [
        (bson!({}), "needs `inputs`"),
        (bson!({ "inputs": [] }), "at least one input"),
        (bson!({ "inputs": "$a" }), "must be an array written out"),
        (bson!({ "inputs": { "$concatArrays": [[1]] } }), "must be an array written out"),
        (bson!({ "inputs": [[1]], "extra": 1 }), "does not take `extra`"),
        (bson!({ "inputs": [[1]], "useLongestLength": 1 }), "must be a boolean"),
        (bson!({ "inputs": [[1]], "useLongestLength": "true" }), "must be a boolean"),
        (bson!({ "inputs": [[1]], "defaults": [0] }), "only with useLongestLength: true"),
        (
            bson!({ "inputs": [[1]], "useLongestLength": false, "defaults": [0] }),
            "only with useLongestLength: true",
        ),
        (
            bson!({ "inputs": [[1], [2]], "useLongestLength": true, "defaults": [0] }),
            "2 inputs, 1 defaults",
        ),
        (
            bson!({ "inputs": [[1]], "useLongestLength": true, "defaults": [0, 1] }),
            "1 inputs, 2 defaults",
        ),
        (
            bson!({ "inputs": [[1]], "useLongestLength": true, "defaults": "$d" }),
            "defaults must be an array written out",
        ),
        (bson!([[1]]), "takes a document"),
    ] {
        let message = parse(spec.clone());
        assert!(message.contains("$zip") && message.contains(want), "{spec:?}: {message}");
    }
}

#[test]
fn zip_with_this_inside_a_map() {
    let row = doc! { "pairs": [{ "k": [1, 2], "v": ["a", "b"] }, { "k": [3], "v": ["c", "d"] }] };
    let expr = doc! { "$map": {
        "input": "$pairs",
        "in": { "$zip": { "inputs": ["$$this.k", "$$this.v"] } },
    } };
    assert_eq!(val(expr, row), bson!([[[1, "a"], [2, "b"]], [[3, "c"]]]));
    let named = doc! { "$map": {
        "input": [1, 2],
        "as": "n",
        "in": { "$zip": { "inputs": [["$$n", "$$n"], [10, 20]] } },
    } };
    assert_eq!(val(named, doc! {}), bson!([[[1, 10], [1, 20]], [[2, 10], [2, 20]]]));
    let with_let = doc! { "$let": {
        "vars": { "d": 0 },
        "in": { "$zip": { "inputs": [[1, 2], [3]], "useLongestLength": true, "defaults": ["$$d", "$$d"] } },
    } };
    assert_eq!(val(with_let, doc! {}), bson!([[1, 3], [2, 0]]));
    let unknown = doc! { "$zip": { "inputs": ["$$nope"] } };
    assert!(Expr::parse(&Bson::Document(unknown)).is_err());
}

#[test]
fn zip_and_sort_array_compose() {
    let expr = doc! { "$sortArray": {
        "input": { "$zip": { "inputs": ["$ks", "$vs"] } },
        "sortBy": -1,
    } };
    assert_eq!(
        val(expr, doc! { "ks": [2, 3, 1], "vs": ["b", "c", "a"] }),
        bson!([[3, "c"], [2, "b"], [1, "a"]])
    );
}

#[test]
fn a_large_zip_is_bounded_before_it_is_built() {
    let array = |n: usize| Bson::Array((0..n as i32).map(Bson::Int32).collect());
    // Under the bound, built whole.
    let Bson::Array(rows) = val(zip(bson!([array(1000), array(1000)])), doc! {}) else {
        panic!("not an array")
    };
    assert_eq!(rows.len(), 1000);
    assert_eq!(rows[999], bson!([999, 999]));
    // Over it: rows times inputs.
    let n = MAX_ZIP_CELLS / 4 + 1;
    let four = |n: usize| bson!([array(n), array(n), array(n), array(n)]);
    let message = err(zip(four(n)), doc! {});
    assert!(message.contains("$zip") && message.contains("over the limit"), "{message}");
    // Exactly at the bound is allowed.
    let Bson::Array(rows) = val(zip(four(n - 1)), doc! {}) else { panic!("not an array") };
    assert_eq!(rows.len(), n - 1);
    // The longest length counts the padding rows too.
    let message = err(zip_longest(bson!([array(MAX_ZIP_CELLS / 2 + 1), []]), None), doc! {});
    assert!(message.contains("over the limit"), "{message}");
}

// ---------------------------------------------------------------------------
// Where expressions work
// ---------------------------------------------------------------------------

fn run(pipeline: &[Document], docs: Vec<Document>) -> Vec<Document> {
    let mut docs = docs;
    for stage in aggregate::parse(pipeline).unwrap_or_else(|e| panic!("{pipeline:?}: {e}")) {
        docs = aggregate::apply(&stage, docs, &Limits::default()).unwrap();
    }
    docs
}

#[test]
fn both_work_in_project_add_fields_group_and_match_expr() {
    let rows = vec![
        doc! { "_id": 1, "g": "x", "xs": [3, 1, 2], "ys": ["a", "b"] },
        doc! { "_id": 2, "g": "x", "xs": [9, 8], "ys": ["c"] },
        doc! { "_id": 3, "g": "y", "xs": null, "ys": ["d"] },
    ];
    let projected = run(
        &[doc! { "$project": {
            "s": { "$sortArray": { "input": "$xs", "sortBy": 1 } },
            "z": { "$zip": { "inputs": ["$xs", "$ys"] } },
        } }],
        rows.clone(),
    );
    assert_eq!(projected[0], doc! { "_id": 1, "s": [1, 2, 3], "z": [[3, "a"], [1, "b"]] });
    assert_eq!(projected[2], doc! { "_id": 3, "s": null, "z": null });

    let added = run(
        &[doc! { "$addFields": { "top": { "$arrayElemAt": [
        { "$sortArray": { "input": "$xs", "sortBy": -1 } }, 0] } } }],
        rows.clone(),
    );
    assert_eq!(added[0].get("top"), Some(&Bson::Int32(3)));
    assert_eq!(added[1].get("top"), Some(&Bson::Int32(9)));

    let grouped = run(
        &[
            doc! { "$group": {
                "_id": "$g",
                "first": { "$first": { "$sortArray": { "input": "$xs", "sortBy": 1 } } },
                "pairs": { "$push": { "$zip": { "inputs": ["$xs", "$ys"] } } },
            } },
            doc! { "$sort": { "_id": 1 } },
        ],
        rows.clone(),
    );
    assert_eq!(grouped[0].get("first"), Some(&bson!([1, 2, 3])));
    assert_eq!(grouped[0].get("pairs"), Some(&bson!([[[3, "a"], [1, "b"]], [[9, "c"]]])));

    let matched = run(
        &[doc! { "$match": { "$expr": { "$eq": [
        { "$arrayElemAt": [{ "$sortArray": { "input": "$xs", "sortBy": 1 } }, 0] }, 8] } } }],
        rows,
    );
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].get("_id"), Some(&Bson::Int32(2)));
}
