//! The reference's reordering of `$skip` and `$limit` (ADR-270): a run of them is one window,
//! and the window moves ahead of the stages that neither filter nor change the number of
//! documents. Each row below is a row of the reference's probes (`adr270-reorder.js` and
//! `adr270-reorder2.js`), whose answers `tests/api.rs` and `exec/stream_tests.rs` hold the
//! executor to; these hold the rewrite to the stages that come out.

use bson::{Document, doc};

use super::*;

/// The stages a pipeline parses to, as `$name` or `$name n`.
fn shape(specs: &[Document]) -> Vec<String> {
    let stages = parse(specs).expect("a valid pipeline");
    stages.iter().map(describe).collect()
}

fn describe(stage: &Stage) -> String {
    match stage {
        Stage::Skip(n) | Stage::Limit(n) => format!("{} {n}", stage.name()),
        other => other.name().to_string(),
    }
}

fn div() -> Document {
    doc! { "$divide": [1, "$d"] }
}

/// Stages the reference moves a window ahead of, each able to fail on the document it is given.
fn movable() -> Vec<(&'static str, Document)> {
    vec![
        ("$project", doc! { "$project": { "x": div() } }),
        ("$addFields", doc! { "$addFields": { "x": div() } }),
        ("$set", doc! { "$set": { "x": div() } }),
        (
            "$replaceRoot",
            doc! { "$replaceRoot": { "newRoot": { "$mergeObjects": ["$$ROOT", { "x": div() }] } } },
        ),
        ("$replaceWith", doc! { "$replaceWith": { "$mergeObjects": ["$$ROOT", { "x": div() }] } }),
        ("$unset", doc! { "$unset": "s" }),
        (
            "$lookup",
            doc! { "$lookup": { "from": "c", "let": { "d": "$d" },
            "pipeline": [{ "$match": { "$expr": { "$gt": [{ "$divide": [1, "$$d"] }, 0] } } }, { "$limit": 1 }],
            "as": "l" } },
        ),
        (
            "$lookup",
            doc! { "$lookup": { "from": "c", "localField": "a", "foreignField": "a", "as": "l" } },
        ),
    ]
}

/// Stages a window stays behind.
fn blocking() -> Vec<(&'static str, Document)> {
    vec![
        ("$redact", doc! { "$redact": { "$cond": [{ "$gt": [div(), 0] }, "$$KEEP", "$$PRUNE"] } }),
        ("$match", doc! { "$match": { "$expr": { "$gt": [div(), 0] } } }),
        ("$match", doc! { "$match": { "a": { "$gte": 0 } } }),
        ("$sort", doc! { "$sort": { "a": 1 } }),
        ("$unwind", doc! { "$unwind": "$arr" }),
        ("$group", doc! { "$group": { "_id": "$a", "x": { "$first": div() } } }),
        ("$sample", doc! { "$sample": { "size": 100 } }),
        ("$sortByCount", doc! { "$sortByCount": "$a" }),
        (
            "$setWindowFields",
            doc! { "$setWindowFields": { "sortBy": { "a": 1 }, "output": { "x": { "$sum": div() } } } },
        ),
        (
            "$graphLookup",
            doc! { "$graphLookup": { "from": "c", "startWith": div(), "connectFromField": "a",
            "connectToField": "a", "as": "g", "maxDepth": 0 } },
        ),
        ("$unionWith", doc! { "$unionWith": "c" }),
        ("$facet", doc! { "$facet": { "x": [{ "$count": "n" }] } }),
        ("$bucketAuto", doc! { "$bucketAuto": { "groupBy": "$a", "buckets": 2 } }),
        ("$count", doc! { "$count": "n" }),
    ]
}

#[test]
fn a_window_moves_ahead_of_each_stage_that_neither_filters_nor_changes_the_count() {
    for (name, stage) in movable() {
        // `$set`, `$unset` and `$replaceWith` are the stages they are another name for.
        let parsed = match name {
            "$set" => "$addFields",
            "$unset" => "$project",
            "$replaceWith" => "$replaceRoot",
            other => other,
        };
        assert_eq!(
            shape(&[stage.clone(), doc! { "$skip": 5 }, doc! { "$limit": 2 }]),
            ["$skip 5", "$limit 2", parsed],
            "{name}"
        );
        // A skip alone, and a limit alone.
        assert_eq!(shape(&[stage.clone(), doc! { "$skip": 5 }]), ["$skip 5", parsed], "{name}");
        assert_eq!(shape(&[stage, doc! { "$limit": 5 }]), ["$limit 5", parsed], "{name}");
    }
}

#[test]
fn an_empty_match_is_passed_unless_it_is_the_filter_of_the_source() {
    // The reference drops a `$match` of nothing, so a window is not held behind it. One that
    // the pipeline begins with is the filter of its source and stays where it is.
    let nothing = doc! { "$match": {} };
    let add = doc! { "$addFields": { "x": 1 } };
    let window = [doc! { "$skip": 5 }, doc! { "$limit": 2 }];
    let with = |before: Vec<Document>| {
        let mut specs = before;
        specs.extend(window.clone());
        shape(&specs)
    };
    assert_eq!(
        with(vec![add.clone(), nothing.clone()]),
        ["$skip 5", "$limit 2", "$addFields", "$match"]
    );
    assert_eq!(with(vec![nothing.clone()]), ["$match", "$skip 5", "$limit 2"]);
    assert_eq!(
        with(vec![doc! { "$match": { "a": 1 } }, nothing.clone()]),
        ["$match", "$match", "$skip 5", "$limit 2"]
    );
    // After a stage that is not a `$match` it is not the source's.
    assert_eq!(
        with(vec![doc! { "$match": { "a": 1 } }, add, nothing]),
        ["$match", "$skip 5", "$limit 2", "$addFields", "$match"]
    );
    // One that filters is never passed.
    assert_eq!(
        with(vec![doc! { "$addFields": { "x": 1 } }, doc! { "$match": { "a": 1 } }]),
        ["$addFields", "$match", "$skip 5", "$limit 2"]
    );
}

#[test]
fn a_window_stays_behind_each_stage_that_filters_or_changes_the_count() {
    for (name, stage) in blocking() {
        assert_eq!(
            shape(&[stage.clone(), doc! { "$skip": 5 }, doc! { "$limit": 2 }]),
            [name, "$skip 5", "$limit 2"],
            "{name}"
        );
        // And one that has passed what it can stops at it.
        assert_eq!(
            shape(&[
                stage,
                doc! { "$addFields": { "x": 1 } },
                doc! { "$project": { "x": 1 } },
                doc! { "$limit": 2 },
            ]),
            [name, "$limit 2", "$addFields", "$project"],
            "{name}"
        );
    }
}

#[test]
fn a_window_moves_over_a_run_of_such_stages_and_stops_at_the_first_that_blocks() {
    assert_eq!(
        shape(&[
            doc! { "$sort": { "a": 1 } },
            doc! { "$addFields": { "x": div() } },
            doc! { "$project": { "x": 1 } },
            doc! { "$replaceWith": "$$ROOT" },
            doc! { "$skip": 5 },
            doc! { "$limit": 2 },
        ]),
        ["$sort", "$skip 5", "$limit 2", "$addFields", "$project", "$replaceRoot"]
    );
    // Blocked by a stage in the middle of the run.
    assert_eq!(
        shape(&[
            doc! { "$addFields": { "x": div() } },
            doc! { "$match": { "a": 1 } },
            doc! { "$project": { "x": 1 } },
            doc! { "$limit": 2 },
        ]),
        ["$addFields", "$match", "$limit 2", "$project"]
    );
    // A `$skip` that comes after a stage that follows the first window joins it.
    assert_eq!(
        shape(&[
            doc! { "$addFields": { "x": div() } },
            doc! { "$skip": 5 },
            doc! { "$project": { "a": 1 } },
            doc! { "$limit": 2 },
        ]),
        ["$skip 5", "$limit 2", "$addFields", "$project"]
    );
}

#[test]
fn a_run_of_skips_and_limits_is_one_window() {
    let add = doc! { "$addFields": { "x": div() } };
    let with = |window: Vec<Document>| {
        let mut specs = vec![add.clone()];
        specs.extend(window);
        shape(&specs)
    };
    // skip 2 + skip 3 = skip 5.
    assert_eq!(
        with(vec![doc! { "$skip": 2 }, doc! { "$skip": 3 }, doc! { "$limit": 4 }]),
        ["$skip 5", "$limit 4", "$addFields"]
    );
    // limit 10 then limit 3 = limit 3; and limit 3 then skip 5 takes nothing, which is a `$limit` of
    // 0 alone: what it drops is of no account.
    assert_eq!(
        with(vec![doc! { "$limit": 10 }, doc! { "$limit": 3 }, doc! { "$skip": 5 }]),
        ["$limit 0", "$addFields"]
    );
    // limit 8 then skip 5 = skip 5, limit 3.
    assert_eq!(
        with(vec![doc! { "$limit": 8 }, doc! { "$skip": 5 }]),
        ["$skip 5", "$limit 3", "$addFields"]
    );
    // A skip, a limit and a skip again: the second drops from what the first took.
    assert_eq!(
        with(vec![doc! { "$skip": 1 }, doc! { "$limit": 10 }, doc! { "$skip": 4 }]),
        ["$skip 5", "$limit 6", "$addFields"]
    );
    assert_eq!(
        with(vec![doc! { "$skip": 1 }, doc! { "$limit": 3 }, doc! { "$limit": 5 }]),
        ["$skip 1", "$limit 3", "$addFields"]
    );
    // A skip of nothing is no window; the stages it was between are one.
    assert_eq!(with(vec![doc! { "$skip": 0 }]), ["$addFields"]);
    assert_eq!(
        shape(&[doc! { "$limit": 5 }, doc! { "$skip": 0 }, doc! { "$limit": 3 }]),
        ["$limit 3"]
    );
    // Windows that nothing separates are coalesced wherever they are, in front of a stage that
    // blocks too.
    assert_eq!(
        shape(&[
            doc! { "$sort": { "a": 1 } },
            doc! { "$skip": 1 },
            doc! { "$skip": 1 },
            doc! { "$limit": 4 }
        ]),
        ["$sort", "$skip 2", "$limit 4"]
    );
    // The sums do not overflow.
    assert_eq!(
        shape(&[
            doc! { "$skip": i64::MAX },
            doc! { "$skip": i64::MAX },
            doc! { "$skip": i64::MAX }
        ]),
        [format!("$skip {}", usize::MAX)]
    );
}

#[test]
fn a_window_that_takes_nothing_is_a_limit_of_nothing_alone() {
    // Behind a `$sort` too: a `$skip` before the `$limit 0` would be documents for the sort to keep.
    assert_eq!(
        shape(&[
            doc! { "$sort": { "a": 1 } },
            doc! { "$limit": 150_000 },
            doc! { "$skip": 200_000 }
        ]),
        ["$sort", "$limit 0"]
    );
    assert_eq!(
        shape(&[doc! { "$skip": 7 }, doc! { "$limit": 3 }, doc! { "$skip": 9 }]),
        ["$limit 0"]
    );
    // Taking something is a skip and a limit.
    assert_eq!(
        shape(&[
            doc! { "$sort": { "a": 1 } },
            doc! { "$limit": 150_000 },
            doc! { "$skip": 100_000 }
        ]),
        ["$sort", "$skip 100000", "$limit 50000"]
    );
}

#[test]
fn the_rewrite_is_the_same_pipeline_run_again() {
    // It is applied once, at the parse; a pipeline that has come out of it comes out of it
    // unchanged, so the stages `explain` reports are the stages that run.
    let specs = [
        doc! { "$addFields": { "x": div() } },
        doc! { "$limit": 8 },
        doc! { "$skip": 5 },
        doc! { "$project": { "a": 1 } },
        doc! { "$limit": 2 },
        doc! { "$sort": { "a": 1 } },
        doc! { "$skip": 1 },
    ];
    let once = shape(&specs);
    assert_eq!(once, ["$skip 5", "$limit 2", "$addFields", "$project", "$sort", "$skip 1"]);
    let written: Vec<Document> = once
        .iter()
        .map(|s| match s.split_once(' ') {
            Some((name, n)) => doc! { name: n.parse::<i64>().unwrap() },
            None => specs
                .iter()
                .find(|d| d.keys().next().map(String::as_str) == Some(s))
                .unwrap()
                .clone(),
        })
        .collect();
    assert_eq!(shape(&written), once);
}

#[test]
fn sub_pipelines_are_rewritten_where_they_are_parsed() {
    let window = [doc! { "$project": { "a": 1 } }, doc! { "$skip": 1 }, doc! { "$limit": 2 }];
    // A `$facet` branch.
    let Stage::Facet(facets) =
        parse(&[doc! { "$facet": { "x": window.to_vec() } }]).unwrap().remove(0)
    else {
        panic!("a $facet")
    };
    assert_eq!(
        facets[0].1.iter().map(describe).collect::<Vec<_>>(),
        ["$skip 1", "$limit 2", "$project"]
    );
    // A `$lookup` sub-pipeline, and a `$unionWith`'s.
    let Stage::Lookup { join: Join::Pipeline { stages, .. }, .. } = parse(&[doc! { "$lookup": {
    "from": "c", "pipeline": window.to_vec(), "as": "l" } }])
    .unwrap()
    .remove(0) else {
        panic!("a $lookup")
    };
    assert_eq!(
        stages.iter().map(describe).collect::<Vec<_>>(),
        ["$skip 1", "$limit 2", "$project"]
    );
    let Stage::UnionWith(union) = parse(&[doc! { "$unionWith": { "coll": "c",
    "pipeline": window.to_vec() } }])
    .unwrap()
    .remove(0) else {
        panic!("a $unionWith")
    };
    assert_eq!(
        union.stages.iter().map(describe).collect::<Vec<_>>(),
        ["$skip 1", "$limit 2", "$project"]
    );
}

#[test]
fn a_window_does_not_move_ahead_of_the_source_or_past_a_stage_that_writes() {
    // `$vectorSearch` is the source and stays first; the window is behind it.
    assert_eq!(
        shape(&[
            doc! { "$vectorSearch": { "vector": [1.0, 0.0], "k": 5 } },
            doc! { "$project": { "name": 1 } },
            doc! { "$skip": 1 },
            doc! { "$limit": 2 },
        ]),
        ["$vectorSearch", "$skip 1", "$limit 2", "$project"]
    );
    // A `$out` is last, and the window before it moves over what is before it only.
    assert_eq!(
        shape(&[doc! { "$project": { "a": 1 } }, doc! { "$limit": 2 }, doc! { "$out": "o" }]),
        ["$limit 2", "$project", "$out"]
    );
    assert_eq!(
        shape(&[
            doc! { "$project": { "a": 1 } },
            doc! { "$skip": 2 },
            doc! { "$merge": { "into": "o" } }
        ]),
        ["$skip 2", "$project", "$merge"]
    );
}

#[test]
fn the_stage_cap_counts_the_pipeline_as_written() {
    // 1,001 `$skip`s are refused before they are one window.
    let skips = vec![doc! { "$skip": 1 }; 1_001];
    assert!(parse(&skips).is_err());
    assert_eq!(shape(&skips[..1_000]), ["$skip 1000"]);
}

/// Run the pure pipeline over `n` documents `{_id, d, a, arr, s}` with `d: 0` at `zero`.
fn answer(specs: &[Document], n: i64, zero: i64) -> Result<Vec<i64>> {
    let stages = parse(specs)?;
    let limits = Limits::default();
    let mut docs: Vec<Document> = (0..n)
        .map(|i| doc! { "_id": i, "d": i64::from(i != zero), "a": i, "arr": [i, i + 1], "s": "x" })
        .collect();
    for stage in &stages {
        docs = apply(stage, docs, &limits)?;
    }
    Ok(docs.iter().map(|d| d.get_i64("_id").unwrap()).collect())
}

#[test]
fn the_answers_are_the_references() {
    // `adr270-reorder.js`, `adr270-reorder2.js`: 20 documents, the failing one is number 3, and
    // a window that drops it moves ahead of the stage that fails on it.
    let window = || [doc! { "$skip": 5 }, doc! { "$limit": 2 }];
    for (name, stage) in movable().into_iter().filter(|(_, stage)| !stage.contains_key("$lookup")) {
        let mut specs = vec![stage];
        specs.extend(window());
        assert_eq!(answer(&specs, 20, 3).unwrap(), [5, 6], "{name}");
    }
    // Behind a stage that blocks, the failure is raised (or the answer is the stage's own).
    for (name, stage) in [("$redact", 0), ("$match", 1), ("$group", 5)]
        .map(|(name, at)| (name, blocking().remove(at).1))
    {
        let mut specs = vec![stage];
        specs.extend(window());
        let err = answer(&specs, 20, 3).unwrap_err();
        assert!(err.to_string().contains("$divide"), "{name}: {err}");
    }
    let add = doc! { "$addFields": { "x": div() } };
    // Not moved past a `$sort` or an `$unwind`: the failing stage runs on every document.
    for blocker in [doc! { "$sort": { "a": 1 } }, doc! { "$unwind": "$arr" }] {
        let specs = [add.clone(), blocker, doc! { "$skip": 5 }, doc! { "$limit": 2 }];
        assert!(answer(&specs, 20, 3).is_err());
    }
    // `limit 8` then `skip 5` takes three; `limit 10`, `limit 3`, `skip 5` takes none; two skips.
    assert_eq!(
        answer(&[add.clone(), doc! { "$limit": 8 }, doc! { "$skip": 5 }], 20, 3).unwrap(),
        [5, 6, 7]
    );
    assert_eq!(
        answer(
            &[add.clone(), doc! { "$limit": 10 }, doc! { "$limit": 3 }, doc! { "$skip": 5 }],
            20,
            3
        )
        .unwrap(),
        Vec::<i64>::new()
    );
    assert_eq!(
        answer(
            &[add.clone(), doc! { "$skip": 2 }, doc! { "$skip": 3 }, doc! { "$limit": 4 }],
            20,
            3
        )
        .unwrap(),
        [5, 6, 7, 8]
    );
    // A stage between the skip and the limit: the window is still before the failing stage.
    assert_eq!(
        answer(
            &[
                add.clone(),
                doc! { "$skip": 5 },
                doc! { "$project": { "a": 1 } },
                doc! { "$limit": 2 }
            ],
            20,
            3
        )
        .unwrap(),
        [5, 6]
    );
    // A failure inside the window is raised.
    let inside = doc! { "$addFields": { "x": { "$divide": [1, { "$cond": [{ "$eq": ["$a", 6] }, 0, 1] }] } } };
    assert!(answer(&[inside, doc! { "$skip": 5 }, doc! { "$limit": 3 }], 20, -1).is_err());
    // A `$project` then a skip: the plain answer.
    assert_eq!(
        answer(&[doc! { "$project": { "a": 1 } }, doc! { "$skip": 18 }], 20, -1).unwrap(),
        [18, 19]
    );
}
