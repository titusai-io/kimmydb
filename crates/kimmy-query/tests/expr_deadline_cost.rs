//! What the deadline's bookkeeping costs an expression (ADR-266, the delta review's m7).
//!
//! Each case is evaluated a million times, best of seven runs, with no deadline in scope and
//! with one an hour away; the table is printed. It is a measurement, not a test, so it is
//! ignored; run it in a release build:
//!
//! ```text
//! cargo test --release -p kimmy-query --test expr_deadline_cost -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use kimmy_core::deadline::Deadline;
use kimmy_query::expr::Expr;

/// Evaluations in a run of a cheap expression; a dear one does fewer, so that a run takes
/// about a third of a second at the most.
const EVALUATIONS: usize = 1_000_000;
const RUNS: usize = 7;

fn expr(spec: Document) -> Expr {
    Expr::parse(&Bson::Document(spec)).unwrap()
}

fn best_of_runs(expr: &Expr, document: &Document, deadline: bool, evaluations: usize) -> Duration {
    let _scope = deadline.then(|| Deadline::scope(Some(Duration::from_secs(3600))));
    (0..RUNS)
        .map(|_| {
            let started = Instant::now();
            for _ in 0..evaluations {
                std::hint::black_box(expr.eval(std::hint::black_box(document)).unwrap());
            }
            started.elapsed()
        })
        .min()
        .unwrap()
}

#[test]
#[ignore = "a measurement: run it in a release build"]
fn what_the_deadline_costs_an_expression() {
    let document = doc! {
        "a": 3, "b": 4, "c": 5, "s": "hello world",
        "arr": [1, 2, 3, 4, 5], "arr2": [1, 2, 3, 4, 6],
        "mid": (0..200).collect::<Vec<i32>>(), "mid2": (1..201).collect::<Vec<i32>>(),
        "big": (0..2_000).collect::<Vec<i32>>(),
    };
    let twenty = |make: &dyn Fn(i32) -> Bson| Bson::Array((0..20).map(make).collect());
    let cases: Vec<(&str, Document)> = vec![
        ("$add of $multiply", doc! { "$add": [{ "$multiply": ["$a", "$b"] }, "$c"] }),
        ("$and of 20 $eq", doc! { "$and": twenty(&|i| Bson::Document(doc! { "$eq": ["$a", i] })) }),
        ("$concat of $toUpper", doc! { "$concat": [{ "$toUpper": "$s" }, "-", "$s"] }),
        (
            "$cond of $gt and $size",
            doc! { "$cond": [{ "$gt": [{ "$size": "$arr" }, 2] }, "big", "small"] },
        ),
        (
            "worst: 20 $eq over small arrays",
            doc! { "$and": twenty(&|_| Bson::Document(doc! { "$eq": ["$arr", "$arr2"] })) },
        ),
        (
            "20 $eq over 200-element arrays",
            doc! { "$and": twenty(&|_| Bson::Document(doc! { "$eq": ["$mid", "$mid2"] })) },
        ),
        ("$in over a 2,000-element array", doc! { "$in": [1_999, "$big"] }),
        (
            "5 $setIntersection of 200",
            doc! { "$size": [{ "$setIntersection": ["$mid", "$mid2"] }] },
        ),
    ];
    println!("\n{:<34} {:>12} {:>12} {:>9}", "case", "no deadline", "deadline", "overhead");
    for (name, spec) in cases {
        let expr = expr(spec);
        let first = Instant::now();
        for _ in 0..1_000 {
            std::hint::black_box(expr.eval(&document).unwrap());
        }
        let one = first.elapsed().as_secs_f64() / 1_000.0;
        let evaluations = ((0.3 / one) as usize).clamp(1_000, EVALUATIONS);
        let off = best_of_runs(&expr, &document, false, evaluations);
        let on = best_of_runs(&expr, &document, true, evaluations);
        let per = |d: Duration| d.as_nanos() as f64 / evaluations as f64;
        println!(
            "{name:<34} {:>9.1} ns {:>9.1} ns {:>8.1}%",
            per(off),
            per(on),
            100.0 * (per(on) / per(off) - 1.0)
        );
    }
}
