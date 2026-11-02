//! `$rand` and `$sampleRate`: the draw is a double in `[0, 1)` from a source a test replaces
//! (`expr::random::with_source`, as `stages::sample` takes its `below`), a fresh one each
//! time it is read, and the filter matches when the draw is below the rate. The reference's
//! refusals (recorded from MongoDB 6.0.5 by `q8-probes/q8d-rand.js` and `q8d-rand2.js`) are
//! pinned in its words, and the system's source is held to be spread over `[0, 1)`, a
//! statistical check with room of six standard deviations.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits};
use crate::expr::{Expr, random};
use crate::filter;

/// A scripted source that answers `values` in order and then repeats the last.
fn script(values: &[f64]) -> impl FnMut() -> f64 + 'static {
    let values = values.to_vec();
    let mut next = 0;
    move || {
        let value = values[next.min(values.len() - 1)];
        next += 1;
        value
    }
}

fn eval(expr: Bson) -> kimmy_core::Result<Bson> {
    Expr::parse(&expr)?.eval(&doc! {})
}

fn rand() -> Bson {
    Bson::Document(doc! { "$rand": {} })
}

fn parse_error(expr: Bson) -> String {
    let text = Expr::parse(&expr).expect_err("must be refused").to_string();
    text.strip_prefix("invalid query: ").unwrap_or(&text).to_string()
}

fn run(pipeline: &[Document], input: Vec<Document>) -> Vec<Document> {
    let mut docs = input;
    for stage in aggregate::parse(pipeline).unwrap_or_else(|e| panic!("{pipeline:?}: {e}")) {
        docs = aggregate::apply(&stage, docs, &Limits::default()).unwrap();
    }
    docs
}

fn numbered(n: i32) -> Vec<Document> {
    (0..n).map(|i| doc! { "_id": i, "g": i % 4, "x": i }).collect()
}

// ---------------------------------------------------------------------------
// $rand
// ---------------------------------------------------------------------------

#[test]
fn rand_is_the_next_number_of_the_source() {
    let draws = random::with_source(script(&[0.25, 0.75, 0.0, 0.999_999_999_999_999_9]), || {
        (0..4).map(|_| eval(rand()).unwrap()).collect::<Vec<_>>()
    });
    assert_eq!(
        draws,
        vec![
            Bson::Double(0.25),
            Bson::Double(0.75),
            Bson::Double(0.0),
            Bson::Double(0.999_999_999_999_999_9)
        ]
    );
}

#[test]
fn rand_is_read_afresh_each_time_it_is_evaluated() {
    // Twice in one document, once per document, and once per group.
    let out = random::with_source(script(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]), || {
        run(
            &[doc! { "$project": { "_id": 0, "a": { "$rand": {} }, "b": { "$rand": {} } } }],
            numbered(3),
        )
    });
    assert_eq!(
        out,
        vec![doc! { "a": 0.1, "b": 0.2 }, doc! { "a": 0.3, "b": 0.4 }, doc! { "a": 0.5, "b": 0.6 }]
    );
    let groups = random::with_source(script(&[0.1, 0.2, 0.3]), || {
        run(&[doc! { "$group": { "_id": { "$rand": {} } } }, doc! { "$count": "n" }], numbered(3))
    });
    assert_eq!(groups, vec![doc! { "n": 3 }]);
}

#[test]
fn rand_in_a_match_expression_decides_each_document() {
    let kept = random::with_source(script(&[0.1, 0.9, 0.4, 0.5, 0.6]), || {
        run(&[doc! { "$match": { "$expr": { "$lt": [{ "$rand": {} }, 0.5] } } }], numbered(5))
    });
    let ids: Vec<i32> = kept.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, vec![0, 2]);
}

#[test]
fn rand_takes_an_empty_document_or_an_empty_array_and_nothing_else() {
    for raw in [Bson::Document(doc! {}), Bson::Array(vec![])] {
        assert!(Expr::parse(&Bson::Document(doc! { "$rand": raw })).is_ok());
    }
    for raw in [Bson::Document(doc! { "a": 1 }), Bson::Array(vec![Bson::Int32(1)])] {
        assert_eq!(
            parse_error(Bson::Document(doc! { "$rand": raw })),
            "$rand does not currently accept arguments"
        );
    }
    for raw in [Bson::Int32(1), Bson::Null, Bson::String("x".into())] {
        assert_eq!(
            parse_error(Bson::Document(doc! { "$rand": raw })),
            "invalid parameter: expected an object ($rand)"
        );
    }
}

#[test]
fn rand_is_a_double_in_zero_to_one_spread_over_the_range() {
    let draws: Vec<f64> = (0..2000)
        .map(|_| match eval(rand()).unwrap() {
            Bson::Double(d) => d,
            other => panic!("not a double: {other:?}"),
        })
        .collect();
    assert!(draws.iter().all(|d| (0.0..1.0).contains(d)));
    let mean = draws.iter().sum::<f64>() / draws.len() as f64;
    // The mean of 2000 draws has a standard deviation of 0.0065.
    assert!((0.45..0.55).contains(&mean), "{mean}");
    // No two alike, and every tenth of the range holds some.
    let mut sorted = draws.clone();
    sorted.sort_by(f64::total_cmp);
    sorted.dedup();
    assert_eq!(sorted.len(), 2000);
    for tenth in 0..10 {
        let held = draws.iter().filter(|d| (**d * 10.0) as usize == tenth).count();
        assert!((120..280).contains(&held), "tenth {tenth}: {held}");
    }
}

#[test]
fn the_scripted_source_is_put_back_after_the_closure() {
    random::with_source(script(&[0.5]), || assert_eq!(eval(rand()).unwrap(), Bson::Double(0.5)));
    // The system's source again: not the scripted one's value, every time.
    let after: Vec<Bson> = (0..20).map(|_| eval(rand()).unwrap()).collect();
    assert!(after.iter().any(|d| *d != Bson::Double(0.5)));
    // Also when the closure panics.
    let unwound = std::panic::catch_unwind(|| {
        random::with_source(script(&[0.5]), || panic!("unwound"));
    });
    assert!(unwound.is_err());
    let after: Vec<Bson> = (0..20).map(|_| eval(rand()).unwrap()).collect();
    assert!(after.iter().any(|d| *d != Bson::Double(0.5)));
}

// ---------------------------------------------------------------------------
// $sampleRate
// ---------------------------------------------------------------------------

fn sample(rate: Bson) -> filter::Filter {
    filter::parse(&doc! { "$sampleRate": rate }).unwrap_or_else(|e| panic!("{e}"))
}

fn matches_with(filter: &filter::Filter, draws: &[f64]) -> Vec<bool> {
    random::with_source(script(draws), || {
        (0..draws.len()).map(|_| filter::matches(filter, &doc! {}).unwrap()).collect()
    })
}

fn refused(filter: Document) -> String {
    let text = filter::parse(&filter).expect_err("must be refused").to_string();
    text.strip_prefix("invalid query: ").unwrap_or(&text).to_string()
}

#[test]
fn a_document_matches_when_the_draw_is_below_the_rate() {
    assert_eq!(
        matches_with(&sample(Bson::Double(0.5)), &[0.0, 0.1, 0.49999, 0.5, 0.9]),
        vec![true, true, true, false, false]
    );
    // Zero matches nothing, even a draw of zero; one matches everything the source can give.
    assert_eq!(matches_with(&sample(Bson::Int32(0)), &[0.0, 0.5, 0.999]), vec![false; 3]);
    assert_eq!(
        matches_with(&sample(Bson::Int32(1)), &[0.0, 0.5, 0.999_999_999_999_999_9]),
        vec![true; 3]
    );
}

#[test]
fn the_rate_is_any_number_type_in_zero_to_one() {
    for (rate, kept) in [
        (Bson::Int32(1), true),
        (Bson::Int64(1), true),
        (Bson::Int64(0), false),
        (Bson::Double(1.0), true),
        (Bson::Double(0.0), false),
        (Bson::Double(-0.0), false),
        (Bson::Decimal128("1".parse().unwrap()), true),
        (Bson::Decimal128("0".parse().unwrap()), false),
    ] {
        assert_eq!(matches_with(&sample(rate.clone()), &[0.5]), vec![kept], "{rate}");
    }
    // A decimal is its nearest double: `0.5` keeps a draw of 0.4 and drops one of 0.6.
    assert_eq!(
        matches_with(&sample(Bson::Decimal128("0.5".parse().unwrap())), &[0.4, 0.6]),
        vec![true, false]
    );
}

#[test]
fn the_rate_is_refused_in_the_references_words() {
    let range = "numeric argument to $sampleRate must be in [0, 1]";
    let numeric = "argument to $sampleRate must be a numeric type";
    for rate in [
        Bson::Double(1.5),
        Bson::Double(-0.5),
        Bson::Int32(-1),
        Bson::Int32(2),
        Bson::Int64(2),
        Bson::Double(f64::NAN),
        Bson::Double(f64::INFINITY),
        Bson::Double(f64::NEG_INFINITY),
        Bson::Decimal128("2".parse().unwrap()),
        Bson::Decimal128("NaN".parse().unwrap()),
    ] {
        assert_eq!(refused(doc! { "$sampleRate": rate.clone() }), range, "{rate}");
    }
    for rate in [
        Bson::String("a".into()),
        Bson::String("0.5".into()),
        Bson::Null,
        Bson::Boolean(true),
        Bson::Boolean(false),
        Bson::Array(vec![Bson::Double(0.5)]),
        Bson::Document(doc! { "a": 1 }),
        Bson::Document(doc! { "$add": [0, 1] }),
        Bson::String("$g".into()),
    ] {
        assert_eq!(refused(doc! { "$sampleRate": rate.clone() }), numeric, "{rate}");
    }
}

#[test]
fn it_combines_with_other_clauses_as_a_clause_of_its_own() {
    let draws = [0.2, 0.2, 0.2, 0.2];
    let d = doc! { "g": 2 };
    let with = |query: Document| {
        let f = filter::parse(&query).unwrap();
        random::with_source(script(&draws), || filter::matches(&f, &d).unwrap())
    };
    assert!(with(doc! { "$sampleRate": 1, "g": 2 }));
    assert!(!with(doc! { "$sampleRate": 1, "g": 3 }));
    assert!(!with(doc! { "$sampleRate": 0, "g": 2 }));
    assert!(with(doc! { "$and": [{ "$sampleRate": 1 }, { "g": 2 }] }));
    assert!(!with(doc! { "$and": [{ "$sampleRate": 0 }, { "g": 2 }] }));
    assert!(with(doc! { "$or": [{ "$sampleRate": 0 }, { "g": 2 }] }));
    assert!(with(doc! { "$or": [{ "$sampleRate": 1 }, { "g": 3 }] }));
    assert!(!with(doc! { "$or": [{ "$sampleRate": 0 }, { "g": 3 }] }));
    assert!(with(doc! { "$nor": [{ "$sampleRate": 0 }] }));
    assert!(!with(doc! { "$nor": [{ "$sampleRate": 1 }] }));
    assert!(!with(doc! { "$and": [{ "$sampleRate": 1 }, { "$sampleRate": 0 }] }));
    assert!(with(doc! { "$and": [{ "$sampleRate": 1 }, { "$sampleRate": 1 }] }));
}

#[test]
fn it_is_the_top_level_of_a_document_filter_only() {
    assert_eq!(refused(doc! { "g": { "$sampleRate": 1 } }), "unsupported operator \"$sampleRate\"");
    assert_eq!(
        refused(doc! { "g": { "$not": { "$sampleRate": 1 } } }),
        "unsupported operator \"$sampleRate\""
    );
    assert_eq!(
        refused(doc! { "g": { "$elemMatch": { "$sampleRate": 1 } } }),
        "$sampleRate can only be applied to the top-level document"
    );
    assert_eq!(
        refused(doc! { "g": { "$elemMatch": { "$and": [{ "$sampleRate": 1 }] } } }),
        "$sampleRate can only be applied to the top-level document"
    );
    assert!(refused(doc! { "$not": { "$sampleRate": 0 } }).contains("$not"));
    let in_expr = Expr::parse(&Bson::Document(doc! { "$sampleRate": 1 }));
    assert!(in_expr.unwrap_err().to_string().contains("not an expression operator"));
    assert_eq!(
        filter::parse_with_vars(&doc! { "$sampleRate": 1 }, &["e".to_string()])
            .unwrap_err()
            .to_string(),
        "invalid query: Cannot use an expression without a top-level field name in arrayFilters"
    );
}

#[test]
fn a_match_stage_keeps_about_the_rate() {
    let input: Vec<Document> = (0..2000).map(|i| doc! { "_id": i }).collect();
    let kept = |rate: f64| run(&[doc! { "$match": { "$sampleRate": rate } }], input.clone()).len();
    assert_eq!(kept(0.0), 0);
    assert_eq!(kept(1.0), 2000);
    // Six standard deviations of a binomial of 2000: 134 at one half, 80 at a tenth.
    assert!((866..1134).contains(&kept(0.5)));
    assert!((120..280).contains(&kept(0.1)));
}

#[test]
fn a_match_stage_decides_each_document_on_its_own_draw() {
    let kept = random::with_source(script(&[0.9, 0.1, 0.9, 0.1, 0.1]), || {
        run(&[doc! { "$match": { "$sampleRate": 0.5 } }], numbered(5))
    });
    let ids: Vec<i32> = kept.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, vec![1, 3, 4]);
}
