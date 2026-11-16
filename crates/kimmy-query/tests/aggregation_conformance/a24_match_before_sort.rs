//! A `$match` that stands right after a `$sort` is applied before it (q7s4l 32), whatever it is:
//! a filter does not depend on the order of the documents, so the answer does not change; only the
//! documents the sort reads do.

use super::common::run;
use bson::{Document, doc};
use kimmy_query::aggregate::{self, Limits};
use proptest::prelude::*;

#[test]
fn a_match_after_a_sort_is_the_first_stage() {
    let parsed = aggregate::parse(&[
        doc! { "$sort": { "n": 1 } },
        doc! { "$match": { "n": { "$gt": 1 } } },
        doc! { "$match": { "n": { "$lt": 9 } } },
        doc! { "$limit": 3 },
    ])
    .unwrap();
    let names: Vec<&str> = parsed.iter().map(aggregate::Stage::name).collect();
    assert_eq!(names, ["$match", "$match", "$sort", "$limit"]);
    // Past two sorts, and a match that follows something else stays where it is.
    let parsed = aggregate::parse(&[
        doc! { "$sort": { "n": 1 } },
        doc! { "$sort": { "s": 1 } },
        doc! { "$match": { "n": 1 } },
        doc! { "$limit": 2 },
        doc! { "$match": { "n": 1 } },
    ])
    .unwrap();
    let names: Vec<&str> = parsed.iter().map(aggregate::Stage::name).collect();
    assert_eq!(names, ["$match", "$sort", "$sort", "$limit", "$match"]);
}

fn applied(stages: &[Document], input: Vec<Document>) -> Vec<Document> {
    // One stage at a time, each parsed alone, so that no move can happen between them.
    let mut current = input;
    for stage in stages {
        let parsed = aggregate::parse(std::slice::from_ref(stage)).unwrap();
        current = aggregate::apply(&parsed[0], current, &Limits::default()).unwrap();
    }
    current
}

proptest! {
    #[test]
    fn the_move_does_not_change_the_answer(
        rows in proptest::collection::vec((0i32..6, 0i32..4, any::<bool>()), 0..20),
        bound in 0i32..6,
        direction in prop_oneof![Just(1i32), Just(-1i32)],
        second in any::<bool>(),
    ) {
        let input: Vec<Document> = rows
            .iter()
            .enumerate()
            .map(|(i, (n, m, flag))| doc! { "_id": i as i32, "n": *n, "m": *m, "f": *flag })
            .collect();
        let mut stages = vec![
            doc! { "$sort": { "n": direction, "_id": 1 } },
            doc! { "$match": { "n": { "$gte": bound } } },
        ];
        if second {
            stages.push(doc! { "$match": { "f": true } });
        }
        stages.push(doc! { "$limit": 5 });
        // As written, stage by stage; and through the pipeline parser, which moves the matches.
        let written = applied(&stages, input.clone());
        let moved = run(&stages, input).unwrap();
        prop_assert_eq!(written, moved);
    }
}
