//! The request deadline inside the pipeline (ADR-266): each loop family stops with
//! `DeadlineExceeded` once the thread's deadline has passed, and nothing changes
//! without one.
//!
//! Two kinds of test, because a stage that checks on entry would hide a loop that
//! does not check at all. A loop is tested **directly** with an already expired
//! deadline (its first tick must fail), and through `apply` with a deadline that
//! passes **while it runs** over input far larger than the deadline allows (the
//! result must be the stop, not the answer).

use std::time::Duration;

use bson::{Bson, Document, doc};
use kimmy_core::Error;
use kimmy_core::deadline::Deadline;

use super::*;

fn expired() -> Deadline {
    Deadline::scope(Some(Duration::ZERO))
}

/// A deadline that has not passed when a stage starts and has passed long before the
/// work over 100,000 documents is done (a few hundred milliseconds unoptimised).
fn soon() -> Deadline {
    Deadline::scope(Some(Duration::from_millis(15)))
}

fn is_deadline<T: std::fmt::Debug>(result: Result<T>) -> bool {
    matches!(result, Err(Error::DeadlineExceeded))
}

fn stage(spec: Document) -> Stage {
    parse(&[spec]).expect("a valid stage").remove(0)
}

fn rows(n: i64) -> Vec<Document> {
    (0..n).map(|i| doc! { "_id": i, "k": i % 7, "v": i, "tags": ["a", "b"] }).collect()
}

#[test]
fn every_stage_looks_at_the_deadline_before_it_starts() {
    let _deadline = expired();
    for spec in [
        doc! { "$match": { "v": { "$gte": 0 } } },
        doc! { "$project": { "v": 1 } },
        doc! { "$addFields": { "w": "$v" } },
        doc! { "$replaceRoot": { "newRoot": "$$ROOT" } },
        doc! { "$sort": { "v": -1 } },
        doc! { "$limit": 5 },
        doc! { "$skip": 1 },
        doc! { "$count": "n" },
        doc! { "$unwind": "$tags" },
        doc! { "$group": { "_id": "$k", "n": { "$sum": 1 } } },
        doc! { "$bucket": { "groupBy": "$v", "boundaries": [0, 10], "default": "other" } },
        doc! { "$bucketAuto": { "groupBy": "$v", "buckets": 2 } },
        doc! { "$facet": { "a": [{ "$limit": 1 }] } },
    ] {
        let name = spec.keys().next().unwrap().clone();
        let result = apply(&stage(spec), rows(3), &Limits::default());
        assert!(is_deadline(result), "{name} ran past an expired deadline");
    }
}

#[test]
fn without_a_deadline_every_stage_runs_as_before() {
    let out = apply(&stage(doc! { "$sort": { "v": -1 } }), rows(3), &Limits::default()).unwrap();
    assert_eq!(out[0].get_i64("v").unwrap(), 2);
    // A deadline well in the future changes nothing either.
    let _deadline = Deadline::scope(Some(Duration::from_secs(3600)));
    let out = apply(&stage(doc! { "$count": "n" }), rows(3), &Limits::default()).unwrap();
    assert_eq!(out, vec![doc! { "n": 3_i64 }]);
}

#[test]
fn match_project_add_fields_and_replace_root_stop_in_their_loops() {
    for spec in [
        doc! { "$match": { "v": { "$gte": 0 } } },
        doc! { "$project": { "v": 1, "w": { "$add": ["$v", 1] } } },
        doc! { "$addFields": { "w": { "$add": ["$v", 1] } } },
        doc! { "$replaceRoot": { "newRoot": { "v": "$v", "w": { "$add": ["$v", 1] } } } },
    ] {
        let name = spec.keys().next().unwrap().clone();
        let input = rows(100_000);
        let stage = stage(spec);
        let _deadline = soon();
        assert!(is_deadline(apply(&stage, input, &Limits::default())), "{name} ran to the end");
    }
}

#[test]
fn unwind_stops_in_its_outer_loop_and_in_the_array_it_fans_out() {
    // Directly, expired: the first document's tick.
    {
        let _deadline = expired();
        let result = unwind(rows(1), "tags", false, None, &Limits::default());
        assert!(is_deadline(result));
    }
    // One document holding a long array: one iteration of the outer loop, many rows. Each
    // row is a copy of the document, so the array is kept short enough that the rows (2,500
    // of 2,500 elements, about 100 MB at most) stay small in memory, and long enough to
    // pass the 1,024th tick. A deadline of a millisecond is passed long before they are made.
    let big = vec![doc! { "_id": 1, "tags": (0..2_500_i64).collect::<Vec<_>>() }];
    let _deadline = Deadline::scope(Some(Duration::from_millis(1)));
    assert!(is_deadline(unwind(big, "tags", false, None, &Limits::default())));
}

#[test]
fn group_stops_in_its_loop() {
    let Stage::Group { id, fields } =
        stage(doc! { "$group": { "_id": "$_id", "n": { "$sum": "$v" } } })
    else {
        unreachable!()
    };
    {
        let _deadline = expired();
        assert!(is_deadline(group(rows(1), &id, &fields, &Limits::default(), &[])));
    }
    // The per-document loop, with the deadline passing while it runs.
    let _deadline = soon();
    assert!(is_deadline(group(rows(100_000), &id, &fields, &Limits::default(), &[])));
}

#[test]
fn bucket_and_bucket_auto_stop_in_their_loops() {
    let Stage::Bucket { group_by, boundaries, default, fields } =
        stage(doc! { "$bucket": { "groupBy": "$v", "boundaries": [0, 50_000, 100_000] } })
    else {
        unreachable!()
    };
    {
        let _deadline = expired();
        let result = bucket(
            rows(1),
            &group_by,
            &boundaries,
            default.as_ref(),
            &fields,
            &Limits::default(),
            &[],
        );
        assert!(is_deadline(result));
    }
    let Stage::BucketAuto { group_by, buckets, fields } =
        stage(doc! { "$bucketAuto": { "groupBy": "$v", "buckets": 4 } })
    else {
        unreachable!()
    };
    {
        let _deadline = expired();
        let result = bucket_auto(rows(1), &group_by, buckets, &fields, &Limits::default(), &[]);
        assert!(is_deadline(result));
    }
    let _deadline = soon();
    let result = bucket_auto(rows(100_000), &group_by, buckets, &fields, &Limits::default(), &[]);
    assert!(is_deadline(result));
}

#[test]
fn facet_looks_between_facets_and_at_each_document_it_measures() {
    let limits = Limits::default();
    let facets = vec![("a".to_string(), vec![]), ("b".to_string(), vec![])];
    {
        let _deadline = expired();
        let result = run_facet(&facets, rows(1), &limits, |_, docs| Ok::<_, Error>(docs));
        assert!(is_deadline(result));
    }
    // One facet whose output is large: the byte count walks every document.
    let one = vec![("a".to_string(), vec![])];
    let _deadline = soon();
    let result = run_facet(&one, rows(100_000), &limits, |_, docs| Ok::<_, Error>(docs));
    assert!(is_deadline(result));
}

#[test]
fn a_sort_stops_while_it_keys_and_while_it_merges() {
    let keys = vec![SortKey { path: "v".into(), descending: true }];
    {
        let _deadline = expired();
        assert!(is_deadline(shape::sort_until_deadline(&keys, rows(1))));
    }
    // More than one run, so the merge passes run as well as the key loop.
    let _deadline = soon();
    assert!(is_deadline(shape::sort_until_deadline(&keys, rows(100_000))));
}

#[test]
fn the_stoppable_sort_orders_exactly_as_the_plain_sort_does_ties_included() {
    // Three keys' worth of structure over more documents than one run, with many ties
    // (`k` has 7 values) so that stability decides most positions.
    let mut input = Vec::new();
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..(shape::SORT_RUN * 3 + 17) as i64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = (x % 7) as i64;
        let doc = match x % 11 {
            0 => doc! { "_id": i, "n": i },          // `k` missing: sorts as null
            1 => doc! { "_id": i, "k": [k, k + 3] }, // an array: by its elements
            2 => doc! { "_id": i, "k": Bson::Null },
            _ => doc! { "_id": i, "k": k, "s": format!("s{}", x % 5) },
        };
        input.push(doc);
    }
    for spec in
        [doc! { "k": 1 }, doc! { "k": -1 }, doc! { "k": 1, "s": -1 }, doc! { "s": -1, "k": -1 }]
    {
        let keys = shape::parse_sort(&spec).unwrap();
        let mut plain = input.clone();
        shape::sort(&keys, &mut plain);
        let checked = shape::sort_until_deadline(&keys, input.clone()).unwrap();
        assert_eq!(checked, plain, "sort {spec}");
    }
    // An empty key list is the input as it came.
    assert_eq!(shape::sort_until_deadline(&[], input.clone()).unwrap(), input);
}

#[test]
fn the_iteration_operators_stop_in_their_loops() {
    let element = doc! { "x": 1 };
    for expr in [
        doc! { "$map": { "input": [1, 2, 3], "as": "n", "in": { "$add": ["$$n", 1] } } },
        doc! { "$filter": { "input": [1, 2, 3], "as": "n", "cond": { "$gt": ["$$n", 1] } } },
        doc! { "$reduce": { "input": [1, 2, 3], "initialValue": 0,
        "in": { "$add": ["$$value", "$$this"] } } },
    ] {
        let name = expr.keys().next().unwrap().clone();
        let parsed = Expr::parse(&Bson::Document(expr)).unwrap();
        // Without a deadline: evaluated.
        assert!(parsed.eval(&element).is_ok(), "{name}");
        let _deadline = expired();
        assert!(is_deadline(parsed.eval(&element)), "{name} ignored an expired deadline");
    }
}

#[test]
fn a_nested_reduce_over_ranges_is_cut_where_the_work_is() {
    // Each of the 100 outer elements runs an inner reduce of 100,000 steps: ten million
    // evaluations of the body, none of the loops long enough to reach a `Ticker`'s own
    // cadence in a short array, which is what the thread's shared count is for.
    let nested = doc! { "$reduce": {
        "input": { "$range": [0, 100] },
        "initialValue": 0,
        "in": { "$add": ["$$value", { "$reduce": {
            "input": { "$range": [0, 100_000] },
            "initialValue": 0,
            "in": { "$add": ["$$value", 1] },
        } }] },
    } };
    let parsed = Expr::parse(&Bson::Document(nested)).unwrap();
    let started = std::time::Instant::now();
    let _deadline = Deadline::scope(Some(Duration::from_millis(50)));
    assert!(is_deadline(parsed.eval(&doc! {})));
    assert!(started.elapsed() < Duration::from_secs(10), "stopped promptly");
}

#[test]
fn short_arrays_in_many_documents_still_reach_the_cadence() {
    // Three elements a document: no single `$map` reaches 1,024 ticks, so only a count
    // kept across calls can see the clock.
    let parsed = Expr::parse(&Bson::Document(
        doc! { "$map": { "input": [1, 2, 3], "as": "n", "in": { "$add": ["$$n", 1] } } },
    ))
    .unwrap();
    let _deadline = Deadline::scope(Some(Duration::from_millis(1)));
    let started = std::time::Instant::now();
    let mut stopped = false;
    for _ in 0..5_000_000 {
        if parsed.eval(&doc! {}).is_err() {
            stopped = true;
            break;
        }
        if started.elapsed() > Duration::from_secs(20) {
            break;
        }
    }
    assert!(stopped, "five million evaluations ran under a 1 ms deadline");
}
