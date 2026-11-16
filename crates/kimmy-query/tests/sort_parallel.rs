//! Sorting on several keys that read arrays (q7s4k, 140 cases: every pair is a `find` and a
//! `$sort` stage answering the same).
//!
//! Two keys that meet different arrays of a document are parallel and refused, code 2, "cannot
//! sort with keys that are parallel arrays": an empty array counts, a position does not help, two
//! arrays in one element or one sub-document are parallel, and arrays in different elements are
//! not. Keys that read **one** array together make one tuple per element, and the document sorts
//! by the tuple that comes first under the whole pattern with its directions.
//!
//! `data/sort_parallel.tsv` is built from the recording by `data/sort_parallel_gen.py`. A `find`
//! is run here as the executor runs it: the keys of every match are taken with
//! `shape::sort_keys` and compared with `shape::compare_keys`.

mod support;

use bson::{Bson, Document, doc};
use support::{documents, find_ids as find, json, pipeline_ids as pipeline};

#[test]
fn every_recorded_pair_answers_as_the_reference_does() {
    let mut rows = 0;
    for row in include_str!("data/sort_parallel.tsv").lines() {
        let [case, kind, before, command, answer] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of five columns: {row}")
        };
        let docs = documents(&json(before));
        let Bson::Document(command) = json(command) else { panic!("{command}") };
        let got = if kind == "find" { find(&docs, &command) } else { pipeline(&docs, &command) };
        match (answer, got) {
            ("ERR", Err(message)) => {
                assert!(
                    message.ends_with("cannot sort with keys that are parallel arrays"),
                    "q7s4k{case}: {message}"
                );
            }
            ("ERR", Ok(got)) => {
                panic!("q7s4k{case} {kind}: answered {got:?}, the reference refused")
            }
            (want, Ok(got)) => {
                let want: Vec<Bson> = json(want).as_array().unwrap().clone();
                assert_eq!(got, want, "q7s4k{case} {kind}: {command}");
            }
            (_, Err(message)) => panic!("q7s4k{case} {kind}: refused: {message}"),
        }
        rows += 1;
    }
    assert!(rows >= 120, "{rows} rows");
}

#[test]
fn two_paths_through_one_array_sort_by_the_first_tuple_under_the_whole_pattern() {
    // q7s4k (d): documents 1 {a: [{x: 1, y: 2}, {x: 3, y: 0}]}, 2 {a: [{x: 2, y: 1}]},
    // 3 {a: [{x: 1, y: 1}]}.
    let docs = vec![
        doc! { "_id": 1, "a": [{ "x": 1, "y": 2 }, { "x": 3, "y": 0 }] },
        doc! { "_id": 2, "a": [{ "x": 2, "y": 1 }] },
        doc! { "_id": 3, "a": [{ "x": 1, "y": 1 }] },
    ];
    let order = |sort: Document| {
        let command = doc! { "sort": sort };
        find(&docs, &command).unwrap().iter().map(|id| id.as_i32().unwrap()).collect::<Vec<_>>()
    };
    assert_eq!(order(doc! { "a.x": 1, "a.y": 1 }), vec![3, 1, 2]);
    assert_eq!(order(doc! { "a.y": 1, "a.x": 1 }), vec![1, 3, 2]);
    assert_eq!(order(doc! { "a.x": -1, "a.y": 1 }), vec![1, 2, 3]);
    assert_eq!(order(doc! { "a.x": 1, "a.y": -1 }), vec![1, 3, 2]);
    // (g): {a: [1, 2]}, {a: [0, 9]}, {a: [0, 3]} by `a.0, a.1` and by `a.0, a`.
    let docs = vec![
        doc! { "_id": 1, "a": [1, 2] },
        doc! { "_id": 2, "a": [0, 9] },
        doc! { "_id": 3, "a": [0, 3] },
    ];
    let order = |sort: Document| {
        let command = doc! { "sort": sort };
        find(&docs, &command).unwrap().iter().map(|id| id.as_i32().unwrap()).collect::<Vec<_>>()
    };
    assert_eq!(order(doc! { "a.0": 1, "a.1": 1 }), vec![3, 2, 1]);
    assert_eq!(order(doc! { "a.0": 1, "a": 1 }), vec![2, 3, 1]);
}
