//! The request deadline inside `$setWindowFields`, `$densify` and `$fill` (ADR-266).
//!
//! Two kinds of test. With a deadline that has already passed, each stage's own entry point
//! (not `aggregate::apply`, whose stage entry would answer first) stops at the first
//! iteration of its first loop. And with a deadline an hour away, the exact count of ticks
//! the shared count has taken (`deadline::ticks_taken`) proves that each loop ticks: a loop
//! that stopped ticking would take its share off the count, even when the loop before it
//! would still stop the stage. The floors below are the loops that tick once for each
//! document or point, added up.

use std::time::Duration;

use bson::{Bson, Document, doc};
use kimmy_core::Error;
use kimmy_core::deadline::{Deadline, ticks_taken};

use crate::aggregate::{self, Limits, Stage};

const N: usize = 3_000;

fn stage(spec: Document) -> Stage {
    aggregate::parse(&[spec]).expect("a valid stage").remove(0)
}

fn rows(n: usize) -> Vec<Document> {
    (0..n as i64).map(|i| doc! { "_id": i, "k": i % 3, "v": i, "gap": if i % 2 == 0 { Bson::Int64(i) } else { Bson::Null } }).collect()
}

/// Run `f` under a deadline an hour away and say how many ticks it took.
fn ticks_of<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let _deadline = Deadline::scope(Some(Duration::from_secs(3600)));
    let before = ticks_taken();
    let out = f();
    (out, ticks_taken() - before)
}

fn floor(ticks: usize) -> u64 {
    ticks as u64
}

fn window(spec: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let Stage::SetWindowFields(spec) = stage(doc! { "$setWindowFields": spec }) else {
        unreachable!()
    };
    crate::window::apply(&spec, input, &Limits::default(), &[])
}

fn densify(spec: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let Stage::Densify(spec) = stage(doc! { "$densify": spec }) else { unreachable!() };
    crate::densify::apply(&spec, input, &Limits::default())
}

fn fill(spec: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let Stage::Fill(spec) = stage(doc! { "$fill": spec }) else { unreachable!() };
    crate::fill::apply(&spec, input, &Limits::default(), &[])
}

fn expired() -> Deadline {
    Deadline::scope(Some(Duration::ZERO))
}

fn is_deadline<T: std::fmt::Debug>(result: kimmy_core::Result<T>) -> bool {
    matches!(result, Err(Error::DeadlineExceeded))
}

fn sum_spec() -> Document {
    doc! {
        "partitionBy": "$k",
        "sortBy": { "v": 1 },
        "output": { "s": { "$sum": "$v", "window": { "documents": ["unbounded", "current"] } } },
    }
}

#[test]
fn every_window_loop_family_stops_on_its_first_iteration() {
    for spec in [
        sum_spec(),
        // No partition: the whole-input charge loop.
        doc! { "sortBy": { "v": 1 }, "output": { "r": { "$rank": {} } } },
        doc! { "sortBy": { "v": 1 }, "output": { "s": { "$shift": { "output": "$v", "by": 1 } } } },
        doc! { "sortBy": { "v": 1 }, "output": { "e": { "$expMovingAvg": { "input": "$v", "N": 2 } } } },
        doc! { "sortBy": { "v": 1 }, "output": { "d": { "$derivative": { "input": "$v" },
        "window": { "documents": [-1, 0] } } } },
        doc! { "sortBy": { "v": 1 }, "output": { "i": { "$integral": { "input": "$v" },
        "window": { "documents": ["unbounded", "current"] } } } },
        doc! { "sortBy": { "v": 1 }, "output": { "l": { "$locf": "$gap" } } },
        doc! { "sortBy": { "v": 1 }, "output": { "l": { "$linearFill": "$gap" } } },
    ] {
        let name = format!("{spec}");
        let ok = window(spec.clone(), rows(3));
        assert!(ok.is_ok(), "{name}: {ok:?}");
        let _deadline = expired();
        assert!(is_deadline(window(spec, rows(3))), "{name} ran past an expired deadline");
    }
}

#[test]
fn densify_and_fill_stop_on_their_first_iteration() {
    let spec = doc! { "field": "v", "range": { "step": 1, "bounds": "full" } };
    assert!(densify(spec.clone(), rows(3)).is_ok());
    let by_value = doc! { "output": { "gap": { "value": 0 } } };
    let by_method = doc! { "sortBy": { "v": 1 }, "output": { "gap": { "method": "locf" } } };
    assert!(fill(by_value.clone(), rows(3)).is_ok());
    assert!(fill(by_method.clone(), rows(3)).is_ok());
    // A scope each: the shared count restarts with a scope, and reads the clock on its first tick.
    {
        let _deadline = expired();
        assert!(is_deadline(densify(spec, rows(3))));
    }
    {
        let _deadline = expired();
        assert!(is_deadline(fill(by_value, rows(3))));
    }
    let _deadline = expired();
    assert!(is_deadline(fill(by_method, rows(3))));
}

#[test]
fn a_window_over_a_partitioned_sorted_input_ticks_for_each_loop_over_its_documents() {
    // The partition loop, the sort-key loop, the slide of the accumulator and the
    // write-back: four loops over the documents.
    let (out, ticks) = ticks_of(|| window(sum_spec(), rows(N)).unwrap());
    assert_eq!(out.len(), N);
    assert!(ticks >= floor(4 * N), "{ticks} ticks for {N} documents");
}

#[test]
fn the_ranking_and_filling_functions_tick_for_each_document() {
    for (spec, loops) in [
        (doc! { "sortBy": { "v": 1 }, "output": { "r": { "$rank": {} } } }, 4),
        (
            doc! { "sortBy": { "v": 1 }, "output": { "e": { "$expMovingAvg": { "input": "$v", "N": 2 } } } },
            4,
        ),
        (doc! { "sortBy": { "v": 1 }, "output": { "l": { "$locf": "$gap" } } }, 4),
        (
            doc! { "sortBy": { "v": 1 }, "output": { "s": { "$shift": { "output": "$v", "by": 1 } } } },
            4,
        ),
    ] {
        let name = format!("{spec}");
        let (out, ticks) = ticks_of(|| window(spec, rows(N)).unwrap());
        assert_eq!(out.len(), N);
        assert!(ticks >= floor(loops * N), "{name}: {ticks} ticks");
    }
}

#[test]
fn densify_ticks_while_it_counts_and_while_it_generates() {
    // Two documents 6,000 apart make 5,999 points: once in the pass that counts them and once
    // in the pass that makes them, and the documents are read once.
    let input = vec![doc! { "_id": 0, "v": 0 }, doc! { "_id": 1, "v": 6000 }];
    let spec = doc! { "field": "v", "range": { "step": 1, "bounds": "full" } };
    let (out, ticks) = ticks_of(|| densify(spec, input).unwrap());
    assert_eq!(out.len(), 6001);
    assert!(ticks >= floor(2 * 5999), "{ticks} ticks");
}

#[test]
fn fill_ticks_for_each_document_it_fills() {
    let (out, ticks) =
        ticks_of(|| fill(doc! { "output": { "gap": { "value": 0 } } }, rows(N)).unwrap());
    assert_eq!(out.len(), N);
    assert!(ticks >= floor(N), "{ticks} ticks");
    let (_, ticks) = ticks_of(|| {
        fill(doc! { "sortBy": { "v": 1 }, "output": { "gap": { "method": "locf" } } }, rows(N))
            .unwrap()
    });
    assert!(ticks >= floor(4 * N), "{ticks} ticks for locf");
}

#[test]
fn linear_fill_ticks_for_each_document_in_each_of_its_loops() {
    // The partition loop, the sort-key loop, the three passes of the fill and the write-back:
    // what each costs per document, so that the loop that fills the last cells is counted.
    let spec = doc! { "sortBy": { "v": 1 }, "output": { "l": { "$linearFill": "$gap" } } };
    let (out, ticks) = ticks_of(|| window(spec, rows(N)).unwrap());
    assert_eq!(out.len(), N);
    // Nine looks for a document, less the first tick of the scope, which the count does not keep.
    assert!(ticks + 1 >= floor(9 * N), "{ticks} ticks for {N} documents");
}

#[test]
fn the_window_looks_at_the_deadline_between_partitions() {
    // One document in each partition: the look before a partition, and the loops over its one
    // document, six looks in all. Without the look between partitions there are five.
    let partitions = |p: usize| -> Vec<Document> {
        (0..p as i64).map(|i| doc! { "_id": i, "k": i, "v": i }).collect()
    };
    let reads =
        |p: usize| kimmy_core::deadline::looks_of(|| window(sum_spec(), partitions(p)).unwrap()).1;
    let diff = reads(80) - reads(20);
    assert_eq!(diff, 6 * 60, "{diff} looks for sixty more partitions");
}
