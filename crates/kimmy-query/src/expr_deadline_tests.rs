//! The request deadline inside one expression (ADR-266, review q8d3, M4): every operator
//! node ticks the thread's shared count, and a node that reads a big value is charged for the
//! whole of it, nested or not, before it runs. The tests are small and do not wait for
//! seconds: they count ticks (`deadline::ticks_taken`) under a deadline an hour away, or use
//! a deadline of a few tens of milliseconds over work that would take many times that.

use std::str::FromStr;
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use kimmy_core::Error;
use kimmy_core::deadline::{self, Deadline, clock_reads, ticks_taken};

use crate::expr::{Expr, logic_arguments, weigh_within};

fn parse(spec: Document) -> Expr {
    Expr::parse(&Bson::Document(spec)).expect("a valid expression")
}

/// The ticks the shared count takes to evaluate `spec` over `document`, under a deadline an
/// hour away.
fn ticks_of(spec: Document, document: &Document) -> u64 {
    let expr = parse(spec);
    let _deadline = Deadline::scope(Some(Duration::from_secs(3600)));
    let before = ticks_taken();
    expr.eval(document).expect("the expression evaluates");
    ticks_taken() - before
}

/// A document holding one value of `n` elements two levels down, the shape of the review's
/// case 1: its top level is one field, and the weight is all below it.
fn nested(n: i32) -> Document {
    doc! { "p": { "q": (0..n).collect::<Vec<i32>>() } }
}

fn is_deadline<T: std::fmt::Debug>(result: kimmy_core::Result<T>) -> bool {
    matches!(result, Err(Error::DeadlineExceeded))
}

#[test]
fn every_operator_node_ticks_once_and_a_read_of_a_field_or_a_literal_does_not() {
    let empty = doc! {};
    // `$and` and fifty `$eq`: fifty-one nodes, small arguments, nothing charged beyond the tick.
    let eqs: Vec<Bson> = (0..50).map(|_| Bson::Document(doc! { "$eq": [1, 1] })).collect();
    assert_eq!(ticks_of(doc! { "$and": eqs.clone() }, &empty), 51);
    assert_eq!(ticks_of(doc! { "$or": eqs.clone() }, &empty), 2, "$or stops at the first true");
    // Both arms of `Expr::Op`: the lazy `$cond` and `$ifNull`, and the eager `$add`.
    assert_eq!(ticks_of(doc! { "$cond": [{ "$eq": [1, 1] }, { "$add": [1, 2] }, 0] }, &empty), 3);
    assert_eq!(ticks_of(doc! { "$ifNull": [null, { "$add": [1, 2] }] }, &empty), 2);
    // A field, a variable and a literal value are not operations.
    assert_eq!(ticks_of(doc! { "$add": ["$a", 1] }, &doc! { "a": 4 }), 1);
    // The special forms.
    let a = doc! { "a": [3, 1, 2], "s": " x ", "n": 2 };
    assert_eq!(ticks_of(doc! { "$sortArray": { "input": "$a", "sortBy": 1 } }, &a), 1);
    assert_eq!(ticks_of(doc! { "$zip": { "inputs": ["$a", "$a"] } }, &a), 1);
    assert_eq!(ticks_of(doc! { "$trim": { "input": "$s" } }, &a), 1);
    assert_eq!(
        ticks_of(doc! { "$replaceAll": { "input": "$s", "find": "x", "replacement": "y" } }, &a),
        1
    );
    assert_eq!(ticks_of(doc! { "$firstN": { "input": "$a", "n": "$n" } }, &a), 1);
    assert_eq!(ticks_of(doc! { "$stdDevPop": "$a" }, &a), 1);
}

#[test]
fn an_and_of_many_comparisons_of_a_big_value_ticks_at_least_once_for_each_node() {
    // The review's case 1, small: a value of 3,000 elements one level below the top, compared
    // with itself fifty times by one `$and`. The top level of each `$eq` argument is a single
    // field, which is what the old charge saw: nothing ticked and nothing was charged.
    let document = nested(3_000);
    let eqs: Vec<Bson> = (0..50).map(|_| Bson::Document(doc! { "$eq": ["$p", "$p"] })).collect();
    let ticks = ticks_of(doc! { "$and": eqs }, &document);
    // At least one for each of the fifty-one nodes, and each `$eq` is charged its two
    // arguments: the weight of 3,000 elements is walked only as far as the stride that is
    // left (a charge of that much reads the clock whatever else it holds), and no less than
    // the floor of 32, so each of the fifty `$eq` makes a charge of at least 32.
    eprintln!("case 1: {ticks} ticks for 51 nodes ({} a node)", ticks / 51);
    assert!(ticks >= 51, "{ticks} ticks");
    assert!(ticks >= 51 + 50 * 32, "{ticks} ticks: the nested size of each argument is charged");
    // The whole weight of the value, walked with no bound, is what the old charge was: the
    // fields and elements at every level.
    let value = Bson::Document(doc! { "q": (0..3_000).collect::<Vec<i32>>() });
    assert_eq!(weigh_within(&value, 0, usize::MAX, 0), 1 + 3_000);
}

#[test]
fn a_big_value_is_charged_for_what_is_inside_it_at_every_depth() {
    // Documents inside arrays inside documents: 20 x 20 x 20 = 8,000 leaves.
    let leaf: Vec<i32> = (0..20).collect();
    let mid: Vec<Document> = (0..20).map(|_| doc! { "v": leaf.clone() }).collect();
    let top: Vec<Document> = (0..20).map(|_| doc! { "m": mid.clone() }).collect();
    let document = doc! { "t": top };
    // One argument, nothing else to pay for; the weight is the elements and fields at every
    // level, at least the leaves.
    let ticks = ticks_of(doc! { "$size": ["$t"] }, &document);
    assert!(ticks >= 33, "{ticks} ticks: charged, and cut at the stride that was left");
    // Walked with no bound, the weight is every level of it: 20 documents, each with 20
    // `m` documents, each with 20 elements in `v` (the framing fields and the leaves).
    let weight = weigh_within(&document.get("t").unwrap().clone(), 0, usize::MAX, 0);
    assert_eq!(weight, 20 + 20 * (1 + 20 + 20 * (1 + 20)), "{weight}");
    assert!(weight >= 8_000);
}

#[test]
fn an_and_of_many_comparisons_is_cut_within_a_bound_of_nodes() {
    // Fifty thousand comparisons of a 3,000-element value would take many seconds; the
    // deadline is thirty milliseconds. The count of `$and` arguments evaluated says how many
    // nodes ran before the cut, which must be a small share of them.
    let document = nested(3_000);
    let eqs: Vec<Bson> =
        (0..50_000).map(|_| Bson::Document(doc! { "$eq": ["$p", "$p"] })).collect();
    let expr = parse(doc! { "$and": eqs });
    let started = Instant::now();
    let before = logic_arguments();
    let _deadline = Deadline::scope(Some(Duration::from_millis(30)));
    assert!(is_deadline(expr.eval(&document)));
    let took = started.elapsed();
    let ran = logic_arguments() - before;
    eprintln!("case 1 cut: {ran} of 50,000 comparisons ran, cut after {took:?} (deadline 30 ms)");
    assert!(ran < 25_000, "{ran} of 50,000 comparisons ran before the cut");
    assert!(took < Duration::from_secs(3), "cut after {took:?}");
}

#[test]
fn an_expired_deadline_stops_the_first_node() {
    let eqs: Vec<Bson> = (0..100).map(|_| Bson::Document(doc! { "$eq": ["$p", "$p"] })).collect();
    let expr = parse(doc! { "$and": eqs });
    let before = logic_arguments();
    let _deadline = Deadline::scope(Some(Duration::ZERO));
    assert!(is_deadline(expr.eval(&nested(3_000))));
    assert_eq!(logic_arguments() - before, 0, "not one comparison ran");
}

/// Make the shared count believe ticks are cheap (a stride of 512), then let the deadline
/// pass with a full stride still to go: the next read of the clock is 512 ticks away.
fn cheap_phase_then_a_passed_deadline() -> Deadline {
    let deadline = Deadline::scope(Some(Duration::from_millis(400)));
    for _ in 0..100_000 {
        deadline::tick().unwrap();
    }
    // Run to the next read: the count is then set to the stride, which is the most.
    let reads = clock_reads();
    while clock_reads() == reads {
        deadline::tick().unwrap();
    }
    std::thread::sleep(Duration::from_millis(450));
    deadline
}

#[test]
fn a_dear_node_after_a_cheap_phase_is_cut_by_its_own_charge_not_a_stride_later() {
    // The review's case 2, the 512-stride blind spot: after a cheap phase the clock is read
    // once in 512 ticks. A node that reads a big value is charged for it before it runs, so
    // it reads the clock at once; one tick would not have.
    let expr = parse(doc! { "$eq": ["$p", "$p"] });
    {
        let _deadline = cheap_phase_then_a_passed_deadline();
        let before = clock_reads();
        let started = Instant::now();
        assert!(is_deadline(expr.eval(&nested(3_000))));
        eprintln!("case 2: the dear node was cut in {:?} (a node)", started.elapsed());
        assert_eq!(clock_reads() - before, 1, "one read, made by the node's charge");
    }
    // A cheap node in the same state is not cut: it is the stride that spares it.
    let cheap = parse(doc! { "$add": [1, 2] });
    let _deadline = cheap_phase_then_a_passed_deadline();
    assert!(cheap.eval(&doc! {}).is_ok());
}

#[test]
fn a_run_of_cheap_nodes_after_a_cheap_phase_is_cut_within_a_stride_of_nodes() {
    let adds: Vec<Bson> = (0..5_000).map(|_| Bson::Document(doc! { "$add": [1, 2] })).collect();
    let expr = parse(doc! { "$and": adds });
    let _deadline = cheap_phase_then_a_passed_deadline();
    // 5,000 nodes outlast the stride of 512, so one of them reads the clock and ends it.
    assert!(is_deadline(expr.eval(&doc! {})));
}

#[test]
fn the_special_forms_charge_the_size_of_what_they_read() {
    let big: Vec<i32> = (0..4_000).collect();
    let words = "ab ".repeat(8_000);
    let document = doc! { "big": big, "words": words, "n": 5 };
    // The charge is the weight walked as far as the stride that was left: a fresh scope has
    // a stride of one, so the walk goes on to the floor of 32 and one charge of at least that
    // is made. What each form is charged in full is held by `a_charge_at_least_the_stride...`
    // below: the clock is read for the big value, and not for a small one.
    let at_least = |spec: Document, _weight: u64| {
        let ticks = ticks_of(spec.clone(), &document);
        assert!(ticks >= 33, "{spec}: {ticks} ticks, wanted a charge of at least the floor");
    };
    at_least(doc! { "$sortArray": { "input": "$big", "sortBy": -1 } }, 4_000);
    at_least(doc! { "$zip": { "inputs": ["$big", "$big"] } }, 8_000);
    // A text is charged by sixteen bytes: 24,000 bytes is 1,500.
    at_least(doc! { "$replaceAll": { "input": "$words", "find": "a", "replacement": "c" } }, 1_500);
    at_least(doc! { "$replaceOne": { "input": "$words", "find": "a", "replacement": "c" } }, 1_500);
    at_least(doc! { "$trim": { "input": "$words", "chars": " " } }, 1_500);
    at_least(doc! { "$ltrim": { "input": "$words" } }, 1_500);
    at_least(doc! { "$rtrim": { "input": "$words" } }, 1_500);
    at_least(doc! { "$firstN": { "input": "$big", "n": "$n" } }, 4_000);
    at_least(doc! { "$lastN": { "input": "$big", "n": "$n" } }, 4_000);
    at_least(doc! { "$maxN": { "input": "$big", "n": "$n" } }, 4_000);
    at_least(doc! { "$minN": { "input": "$big", "n": "$n" } }, 4_000);
    at_least(doc! { "$stdDevPop": "$big" }, 4_000);
    at_least(doc! { "$stdDevSamp": "$big" }, 4_000);
}

#[test]
fn the_special_forms_are_cut_by_their_own_charge_under_a_passed_deadline() {
    let document =
        doc! { "big": (0..4_000).collect::<Vec<i32>>(), "words": "ab ".repeat(8_000), "n": 5 };
    for spec in [
        doc! { "$sortArray": { "input": "$big", "sortBy": 1 } },
        doc! { "$zip": { "inputs": ["$big", "$big"] } },
        doc! { "$replaceAll": { "input": "$words", "find": "a", "replacement": "c" } },
        doc! { "$trim": { "input": "$words" } },
        doc! { "$firstN": { "input": "$big", "n": "$n" } },
        doc! { "$stdDevSamp": "$big" },
    ] {
        let expr = parse(spec.clone());
        // Past the first tick's read, with the stride at its most and the deadline gone.
        let _deadline = cheap_phase_then_a_passed_deadline();
        let before = clock_reads();
        assert!(is_deadline(expr.eval(&document)), "{spec} ran past the deadline");
        assert!(clock_reads() > before);
    }
}

/// A deadline an hour away, with the stride at its most: after a cheap phase the shared count
/// is full, and 512 ticks are to go before the clock is read again.
fn cheap_phase_under_an_hour() -> Deadline {
    let deadline = Deadline::scope(Some(Duration::from_secs(3600)));
    for _ in 0..100_000 {
        deadline::tick().unwrap();
    }
    let reads = clock_reads();
    while clock_reads() == reads {
        deadline::tick().unwrap();
    }
    assert_eq!(deadline::remaining_stride(), Some(512));
    deadline
}

#[test]
fn weighing_is_exact_below_the_bound_and_stops_at_it() {
    let value = Bson::Document(doc! { "a": (0..100).collect::<Vec<i32>>(), "b": "x".repeat(160) });
    // 2 fields, 100 elements and 160 / 16 of text.
    let full = 2 + 100 + 10;
    assert_eq!(weigh_within(&value, 0, usize::MAX, 0), full);
    assert_eq!(weigh_within(&value, 0, full + 1, 0), full, "below the bound: exact");
    for bound in [1, 2, 3, 50, 102, 103, full] {
        let walked = weigh_within(&value, 0, bound, 0);
        assert!(walked >= bound && walked <= full, "bound {bound}: walked {walked}");
    }
    // Deeper than the depth cap, a value counts as the length of the level it stops at.
    let mut deep = Bson::Array(vec![Bson::Int32(1); 5]);
    for _ in 0..80 {
        deep = Bson::Array(vec![deep]);
    }
    assert_eq!(weigh_within(&deep, 0, usize::MAX, 0), 65);
}

#[test]
fn a_charge_at_least_what_is_left_of_the_stride_reads_the_clock_once_and_a_smaller_one_does_not() {
    // A document of three arrays: 3 fields and the elements, 1,203 in all when walked in full.
    let arrays = |n: i32| {
        let one = || (0..n).collect::<Vec<i32>>();
        doc! { "x": { "a": one(), "b": one(), "c": one() } }
    };
    let (small, big) = (arrays(33), arrays(400));
    let size = parse(doc! { "$objectToArray": "$x" });
    // A weight of 102 is under the 512 ticks that are left: it is charged, and the clock is not
    // read; the stride that is left is 410 less one for the node.
    {
        let _deadline = cheap_phase_under_an_hour();
        let before = clock_reads();
        size.eval(&small).unwrap();
        assert_eq!(clock_reads() - before, 0);
        let left = deadline::remaining_stride().unwrap();
        assert!(left < 512 - 102 && left > 512 - 108, "{left} left");
    }
    // A weight of 1,203 is over it: the clock is read once, by the charge, and the walk
    // stopped at the stride (after the second array: 803), so the ticks taken are that and
    // not 1,203.
    {
        let _deadline = cheap_phase_under_an_hour();
        let (before, taken) = (clock_reads(), ticks_taken());
        size.eval(&big).unwrap();
        assert_eq!(clock_reads() - before, 1, "one read, made by the charge");
        let charged = ticks_taken() - taken;
        assert!((512..1_203).contains(&charged), "{charged} ticks taken: truncated at the stride");
    }
}

#[test]
fn the_charge_is_made_from_a_weight_of_32_and_reads_the_clock_from_the_stride_exactly() {
    // `$objectToArray` of a document of one array: the weight is the field and the elements.
    let with = |elements: i32| doc! { "x": { "a": (0..elements).collect::<Vec<i32>>() } };
    let op = parse(doc! { "$objectToArray": "$x" });
    // 31 is under the floor and is not charged: the node's own tick is all the count takes.
    {
        let _deadline = cheap_phase_under_an_hour();
        let before = deadline::remaining_stride().unwrap();
        op.eval(&with(30)).unwrap();
        assert_eq!(before - deadline::remaining_stride().unwrap(), 1);
    }
    // 32 is the floor and is charged (and the node's tick).
    {
        let _deadline = cheap_phase_under_an_hour();
        let before = deadline::remaining_stride().unwrap();
        op.eval(&with(31)).unwrap();
        assert_eq!(before - deadline::remaining_stride().unwrap(), 33);
    }
    // The node's own tick takes one of the 512 before its arguments are charged, so 511 are
    // left: a weight of 511 (the field and 510 elements) reads the clock, one of 510 does not.
    {
        let _deadline = cheap_phase_under_an_hour();
        let reads = clock_reads();
        op.eval(&with(509)).unwrap();
        assert_eq!(clock_reads() - reads, 0, "510 of the 511 that are left: one short");
    }
    {
        let _deadline = cheap_phase_under_an_hour();
        let reads = clock_reads();
        op.eval(&with(510)).unwrap();
        assert_eq!(clock_reads() - reads, 1, "a charge of at least the stride reads once");
    }
    // The walk is not cut a unit short: a weight of 511 whose last unit is the text after two
    // arrays that came to 510 (3 fields, 254 and 253 elements, 16 bytes of text) still reads.
    {
        let _deadline = cheap_phase_under_an_hour();
        let reads = clock_reads();
        let last_unit = doc! { "x": {
            "a": (0..254).collect::<Vec<i32>>(),
            "b": (0..253).collect::<Vec<i32>>(),
            "c": "x".repeat(16),
        } };
        op.eval(&last_unit).unwrap();
        assert_eq!(clock_reads() - reads, 1, "510 and the text's one");
    }
    // A text is weighed by sixteen bytes, as the argument of an operator that reads it whole.
    let split = parse(doc! { "$split": ["$s", ","] });
    {
        let _deadline = cheap_phase_under_an_hour();
        let reads = clock_reads();
        split.eval(&doc! { "s": "a,".repeat(5_000) }).unwrap();
        assert_eq!(clock_reads() - reads, 1, "10,000 bytes weigh 625");
    }
    {
        let _deadline = cheap_phase_under_an_hour();
        let reads = clock_reads();
        split.eval(&doc! { "s": "a,".repeat(200) }).unwrap();
        assert_eq!(clock_reads() - reads, 0, "400 bytes weigh 25");
    }
}

#[test]
fn without_a_deadline_nothing_is_counted_or_charged_and_every_form_runs_as_before() {
    assert!(!Deadline::is_set());
    let reads = clock_reads();
    let ticks = ticks_taken();
    let document = nested(3_000);
    let value = parse(doc! { "$and": [{ "$eq": ["$p", "$p"] }, { "$sortArray": { "input": "$p.q", "sortBy": -1 } }] })
        .eval(&document)
        .unwrap();
    assert_eq!(value, Bson::Boolean(true));
    assert_eq!((clock_reads(), ticks_taken()), (reads, ticks));
}

#[test]
fn the_iteration_operators_tick_for_each_element_even_when_the_body_is_not_an_operation() {
    // A body that is a variable or a literal is no node, so it ticks nothing of its own: the
    // tick of the iteration is what counts the elements (with an operator in the body, the
    // body's nodes tick as well, which is why this is held with a body that has none).
    let document = doc! { "a": (1..=700).collect::<Vec<i32>>() };
    for (name, spec) in [
        ("$map", doc! { "$map": { "input": "$a", "as": "n", "in": "$$n" } }),
        ("$filter", doc! { "$filter": { "input": "$a", "as": "n", "cond": "$$n" } }),
        ("$reduce", doc! { "$reduce": { "input": "$a", "initialValue": 0, "in": "$$this" } }),
    ] {
        // One tick for the node, one for each of the 700 elements.
        assert_eq!(ticks_of(spec, &document), 701, "{name}");
    }
}

// ---------------------------------------------------------------------------
// A decimal $sin of a huge operand (review q8d, M1)
// ---------------------------------------------------------------------------

/// `{$cond: [{$lt: ["$n", switch]}, 0, {$sin: 1E+6144}]}`: cheap for `switch` documents and then a
/// sine whose operand is reduced modulo `pi/2` with numbers of some 12,000 digits.
fn late_sine() -> Document {
    doc! { "$cond": [
        { "$lt": ["$n", 5000] },
        0,
        { "$sin": { "$literal": bson::Decimal128::from_str("1E+6144").unwrap() } }
    ] }
}

#[test]
fn a_decimal_sine_of_a_huge_operand_is_charged_its_reduction_before_it_runs() {
    // The exponent is the charge: 6,144 ticks, past the stride, so the clock is read.
    let sine = doc! { "$sin": { "$literal": bson::Decimal128::from_str("1E+6144").unwrap() } };
    assert!(ticks_of(sine, &doc! {}) >= 6144);
    let small = doc! { "$sin": { "$literal": bson::Decimal128::from_str("1.5").unwrap() } };
    // The `$sin` and its `$literal` are a tick each, and an operand of exponent 0 is charged none.
    assert_eq!(ticks_of(small, &doc! {}), 2);
}

#[test]
fn a_switch_to_a_huge_decimal_sine_after_thousands_of_cheap_documents_ends_at_the_deadline() {
    use crate::aggregate::{self, Limits};
    let expr = parse(late_sine());
    // `pi` is built once for the process, by the first sine of an operand this large.
    expr.eval(&doc! { "n": 9999 }).expect("a sine");

    let docs: Vec<Document> = (0..12_000).map(|n| doc! { "_id": n, "n": n }).collect();
    let stages = aggregate::parse(&[doc! { "$project": { "x": late_sine() } }]).unwrap();
    let _deadline = Deadline::scope(Some(Duration::from_millis(30)));
    let start = Instant::now();
    let result = aggregate::apply(&stages[0], docs, &Limits::default());
    let took = start.elapsed();
    assert!(is_deadline(result), "the request must end at its deadline");
    // The 5,000 cheap documents take a few milliseconds and the sines the rest of the 30: it
    // ends within about one sine of the deadline, not minutes after it.
    assert!(took < Duration::from_secs(5), "{took:?}");
}
