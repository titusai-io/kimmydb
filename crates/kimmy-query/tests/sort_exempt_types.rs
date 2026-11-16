//! The exemption from the parallel-arrays refusal, by the type of the filter's `_id` (q7s4m, 72
//! cases: 24 `find`, `$sort` stages and `findAndModify` each, the recording `q7s4m.jsonl`, and
//! q7s2z 13-30: an array literal, `{}` and `{$eq: {}}`, with a control): a filter
//! that is exactly one key `_id` whose value is a literal reads the one document by its key and
//! sorts nothing, so the document is returned, **for every type of value but `null`**: a string, an
//! ObjectId, an Int64, a Double (`2.5`, and `2.0` against a stored Int32 2), a Decimal128, a boolean,
//! a date, a document, a binary, a UUID, a `MinKey` and the empty string. Refused, as every filter
//! that is not that shape is: `{_id: null}`, a regular expression in either spelling (`/^s/`,
//! `{$regex: ...}`), `{_id: {$eq: v}}`, `{_id: {$in: [v]}}`, and an extra key beside `_id` in either
//! order. An array as the literal (`{_id: [1, 2]}`, `[]`, `[2]`) selects nothing and raises nothing
//! in the three routes (q7s2z 13-21); `{_id: {}}` is exempt and `{_id: {$eq: {}}}` is refused
//! (q7s2z 22-27). `sort_exempt.rs` holds the numbers (q7s4l).
//!
//! The returned document of a `findAndModify` of q7s2z (rows 15, 18, 21, 24 and 30; row 27 is
//! refused) is compared too: `null` where the filter selects nothing (an array as the `_id`
//! literal), the document after the update where it selects one (`{}`, the control), keys in the
//! order recorded.
//!
//! `data/sort_exempt_types.tsv` is built from the recording by `data/sort_exempt_types_gen.py`.
//! The recordings are typed (the find reader that kept the width; a stored Double 2.0 is answered
//! as `2.0`), so the answers are compared by type and value. **Every failing row is reported, all
//! of them, before the test asserts.**

mod support;

use bson::{Bson, Document};
use support::{documents, find_and_modify_value, find_ids, json, pipeline_ids};

#[test]
fn every_recorded_id_type_answers_as_the_reference_does() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("data/sort_exempt_types.tsv").lines() {
        let [case, kind, before, command, answer] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of five columns: {row}")
        };
        let docs = documents(&json(before));
        let Bson::Document(command): Bson = json(command) else { panic!("{command}") };
        let got: Result<Vec<Bson>, String> = match kind {
            "find" => find_ids(&docs, &command),
            "aggregate" => pipeline_ids(&docs, &command),
            "findAndModify" => find_and_modify_value(&docs, &command)
                .map(|value| vec![value.map_or(Bson::Null, Bson::Document)]),
            other => panic!("{other}"),
        };
        rows += 1;
        let refused =
            |message: &str| message.ends_with("cannot sort with keys that are parallel arrays");
        let wrong = match (answer, &got) {
            ("ERR", Err(message)) if refused(message) => None,
            ("ERR", Err(message)) => Some(format!("refused, but as {message:?}")),
            ("ERR", Ok(got)) => Some(format!("the reference refused it; ours answered {got:?}")),
            ("OK", Ok(_)) => None,
            // the returned document of a findAndModify: recorded `value`, compared by type, value
            // and key order
            (want, Ok(got)) if kind == "findAndModify" => {
                let want = json(want);
                let want = want.as_document().unwrap().get("value").unwrap();
                (got.first().map(|g| g.to_string()) != Some(want.to_string()) || got[0] != *want)
                    .then(|| format!("recorded value {want}, ours {got:?}"))
            }
            (want, Ok(got)) => {
                let want: Vec<Bson> = json(want).as_array().unwrap().clone();
                (got != &want).then(|| format!("recorded {want:?}, ours {got:?}"))
            }
            (want, Err(message)) => Some(format!("recorded {want}, ours refused: {message}")),
        };
        if let Some(wrong) = wrong {
            let filter: &Document = command
                .get_document("filter")
                .or_else(|_| command.get_document("query"))
                .unwrap_or(&command);
            failures.push(format!("{case} {kind} {filter}: {wrong}"));
        }
    }
    assert_eq!(rows, 90, "the generator wrote 90 cases");
    println!("sort_exempt_types: {rows} cases");
    assert!(
        failures.is_empty(),
        "{} of {rows} rows differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
