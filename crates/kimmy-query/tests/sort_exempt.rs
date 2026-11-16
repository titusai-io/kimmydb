//! The exemption from the parallel-arrays refusal (q7s4l, 52 cases: `find`, `$sort` stages and
//! `findAndModify`): a filter that is exactly `{_id: <number>}` reads the one document by its key and
//! sorts nothing, so the document is returned; every other filter that selects it is refused. In
//! a pipeline the `$match` is the first stage and the `$sort` follows it or only a `$skip` stands
//! between; a `$match` right after a `$sort` is applied before it; a `$limit` that keeps the document
//! out leaves the sort nothing to refuse. Only a number is recorded: the other types of `_id` are
//! unrecorded.
//!
//! `data/sort_exempt.tsv` is built from the recording by `data/sort_exempt_gen.py`.

mod support;

use bson::Bson;
use support::{documents, find_and_modify_answers, find_ids, json, pipeline_ids};

#[test]
fn every_recorded_row_answers_as_the_reference_does() {
    let mut rows = 0;
    for row in include_str!("data/sort_exempt.tsv").lines() {
        let [case, kind, before, command, answer] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of five columns: {row}")
        };
        let docs = documents(&json(before));
        let Bson::Document(command) = json(command) else { panic!("{command}") };
        let got = match kind {
            "find" => find_ids(&docs, &command),
            "aggregate" => pipeline_ids(&docs, &command),
            "findAndModify" => find_and_modify_answers(&docs, &command).map(|()| Vec::new()),
            other => panic!("{other}"),
        };
        match (answer, got) {
            ("ERR", Err(message)) => assert!(
                message.ends_with("cannot sort with keys that are parallel arrays"),
                "q7s4l{case}: {message}"
            ),
            ("ERR", Ok(got)) => {
                panic!("q7s4l{case} {kind}: answered {got:?}, the reference refused")
            }
            ("OK", Ok(_)) => {}
            (want, Ok(got)) => {
                let want: Vec<Bson> = json(want).as_array().unwrap().clone();
                assert_eq!(got, want, "q7s4l{case} {kind}: {command}");
            }
            (_, Err(message)) => panic!("q7s4l{case} {kind}: refused: {message}"),
        }
        rows += 1;
    }
    assert_eq!(rows, 52);
}

/// The three shapes of a `$match` and a `$sort` that the move can make or unmake, over a collection
/// where document 2 holds parallel arrays (q7s4l 32 is the first; the third is UNRECORDED).
#[test]
fn the_move_decides_which_pipelines_are_the_exempt_form() {
    let docs = documents(&json(
        r#"[{"_id": 1, "a": 1, "b": 2}, {"_id": 2, "a": [1, 2], "b": [3, 4]}, {"_id": 3, "a": 0, "b": 0}]"#,
    ));
    let run = |text: &str| {
        let Bson::Document(command) = json(&format!(r#"{{"pipeline": {text}}}"#)) else { panic!() };
        pipeline_ids(&docs, &command)
    };
    let sort = r#"{"$sort": {"a": 1, "b": 1, "_id": 1}}"#;
    // A `$match {_id: 2}` after the sort is moved before it and the form is exempt (q7s4l 32).
    assert_eq!(run(&format!(r#"[{sort}, {{"$match": {{"_id": 2}}}}]"#)), Ok(vec![Bson::Int32(2)]));
    assert_eq!(
        run(&format!(r#"[{sort}, {{"$match": {{"_id": 2}}}}, {{"$project": {{"_id": 1}}}}]"#)),
        Ok(vec![Bson::Int32(2)])
    );
    // A `$limit` between the match and the sort keeps the check (q7s4l 28).
    assert!(run(&format!(r#"[{{"$match": {{"_id": 2}}}}, {{"$limit": 1}}, {sort}]"#)).is_err());
    // `[$match {_id: 2}, $sort, $match {...}]` moves to two matches before the sort: not the
    // exempt form, so refused (UNRECORDED).
    let err = run(&format!(
        r#"[{{"$match": {{"_id": 2}}}}, {sort}, {{"$match": {{"a": {{"$exists": true}}}}}}]"#
    ))
    .unwrap_err();
    assert!(err.ends_with("cannot sort with keys that are parallel arrays"), "{err}");
    // A match after the sort that is not the `_id` form leaves the document, and the sort refuses it.
    assert!(run(&format!(r#"[{sort}, {{"$match": {{"a": {{"$exists": true}}}}}}]"#)).is_err());
}
