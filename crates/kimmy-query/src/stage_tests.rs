//! `$sortByCount`, `$replaceWith`, `$unset`, `$sample`, `$redact` and the parse side of
//! `$unionWith` (ADR-263): what they answer and what they refuse. Every expected value is
//! written out by hand from the reference's behaviour.

use bson::{Bson, Document, doc};
use kimmy_core::Error;

use crate::aggregate::{self, Limits, Stage};
use crate::stages;

fn run_with(
    stage: Document,
    input: Vec<Document>,
    limits: &Limits,
) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&[stage])?;
    aggregate::apply(&stages[0], input, limits)
}

fn run(stage: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    run_with(stage, input, &Limits::default())
}

fn refused(stage: Document) -> String {
    aggregate::parse(&[stage]).expect_err("must be refused").to_string()
}

fn kinds(values: &[&str]) -> Vec<Document> {
    values.iter().enumerate().map(|(i, k)| doc! {"_id": i as i32, "k": *k}).collect()
}

// ---------------------------------------------------------------------------
// $sortByCount
// ---------------------------------------------------------------------------

#[test]
fn sort_by_count_counts_each_value_and_puts_the_commonest_first() {
    let out = run(doc! {"$sortByCount": "$k"}, kinds(&["b", "a", "c", "a", "a", "b"])).unwrap();
    // a: 3, b: 2, c: 1. The count is an int, as the reference's is.
    assert_eq!(
        out,
        vec![
            doc! {"_id": "a", "count": 3},
            doc! {"_id": "b", "count": 2},
            doc! {"_id": "c", "count": 1},
        ]
    );
    assert!(out.iter().all(|d| matches!(d.get("count"), Some(Bson::Int32(_)))), "{out:?}");
    // `$group`'s own `{$sum: 1}` stays a long in this build.
    let grouped = run(doc! {"$group": {"_id": "$k", "count": {"$sum": 1}}}, kinds(&["a"])).unwrap();
    assert_eq!(grouped[0].get("count"), Some(&Bson::Int32(1)));
}

#[test]
fn sort_by_count_keeps_first_seen_order_among_equal_counts() {
    // The reference leaves ties unspecified (two runs of it differ); here they stay in the
    // order their value was first seen, which is not a promise: z, y, x all once, then q
    // twice -> q, z, y, x.
    let out = run(doc! {"$sortByCount": "$k"}, kinds(&["z", "y", "q", "x", "q"])).unwrap();
    let ids: Vec<&str> = out.iter().map(|d| d.get_str("_id").unwrap()).collect();
    assert_eq!(ids, ["q", "z", "y", "x"]);
}

#[test]
fn sort_by_count_groups_missing_and_null_together_under_null() {
    let input = vec![doc! {"_id": 1}, doc! {"_id": 2, "k": Bson::Null}, doc! {"_id": 3, "k": 1}];
    let out = run(doc! {"$sortByCount": "$k"}, input).unwrap();
    assert_eq!(out, vec![doc! {"_id": Bson::Null, "count": 2}, doc! {"_id": 1, "count": 1}]);
}

#[test]
fn sort_by_count_takes_an_expression_document() {
    let input = kinds(&["a", "b", "a", "a", "b"]);
    let out = run(doc! {"$sortByCount": {"$toUpper": "$k"}}, input).unwrap();
    assert_eq!(out, vec![doc! {"_id": "A", "count": 3}, doc! {"_id": "B", "count": 2}]);
    // `$$ROOT` groups by the whole document: two distinct documents, one each.
    let out = run(
        doc! {"$sortByCount": "$$ROOT"},
        vec![doc! {"_id": 1, "k": "a"}, doc! {"_id": 2, "k": "b"}, doc! {"_id": 1, "k": "a"}],
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": {"_id": 1, "k": "a"}, "count": 2},
            doc! {"_id": {"_id": 2, "k": "b"}, "count": 1},
        ]
    );
}

#[test]
fn sort_by_count_groups_numbers_of_every_type_by_value() {
    let input = vec![doc! {"k": 1}, doc! {"k": 1.0}, doc! {"k": 2i64}, doc! {"k": 1i64}];
    let out = run(doc! {"$sortByCount": "$k"}, input).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("count"), Some(&Bson::Int32(3)));
    assert_eq!(out[1].get("count"), Some(&Bson::Int32(1)));
}

#[test]
fn sort_by_count_of_nothing_is_nothing() {
    assert_eq!(run(doc! {"$sortByCount": "$k"}, vec![]).unwrap(), Vec::<Document>::new());
}

#[test]
fn sort_by_count_refuses_what_is_not_a_path_or_an_expression_with_the_references_words() {
    // Reference 6.0.5, section by section.
    let kind = "the sortByCount field must be specified as a string or as an object";
    let path = "the sortByCount field must be defined as a $-prefixed path or an expression inside an object";
    for (operand, want) in [
        (Bson::Int32(1), kind),
        (Bson::Double(1.5), kind),
        (Bson::Boolean(true), kind),
        (Bson::Null, kind),
        (Bson::Array(vec![Bson::String("$k".into())]), kind),
        (Bson::Document(doc! {}), path),
        // An object that is not an operator expression is no expression here either.
        (Bson::Document(doc! {"who": "$k"}), path),
        (Bson::String("k".into()), path),
        (Bson::String("".into()), path),
        (Bson::String("$".into()), "'$' by itself is not a valid FieldPath"),
    ] {
        let message = refused(doc! {"$sortByCount": operand.clone()});
        assert_eq!(message, format!("invalid query: {want}"), "{operand:?}");
    }
    // A bad expression inside is that expression's error.
    assert!(aggregate::parse(&[doc! {"$sortByCount": {"$nope": 1}}]).is_err());
}

#[test]
fn sort_by_count_widens_its_count_to_a_long_past_the_int_range() {
    // The count is `$group`'s `{$sum: 1}`, typed by its total (ADR-268).
    let mut total = crate::expr::Total::default();
    total.add(&Bson::Int32(i32::MAX));
    assert_eq!(total.to_bson(), Bson::Int32(i32::MAX));
    total.add(&Bson::Int32(1));
    assert_eq!(total.to_bson(), Bson::Int64(i64::from(i32::MAX) + 1));
}

#[test]
fn sort_by_count_is_held_to_the_group_limit() {
    let err = run_with(
        doc! {"$sortByCount": "$k"},
        kinds(&["a", "b", "c", "d"]),
        &Limits { max_documents: 3 },
    )
    .expect_err("four groups over a limit of three");
    assert!(matches!(&err, Error::Limit(m) if m.contains("$sortByCount")), "{err:?}");
}

// ---------------------------------------------------------------------------
// $replaceWith
// ---------------------------------------------------------------------------

#[test]
fn replace_with_is_replace_root() {
    let input = vec![doc! {"_id": 1, "a": {"x": 1}}, doc! {"_id": 2, "a": {"x": 2}}];
    let with = run(doc! {"$replaceWith": "$a"}, input.clone()).unwrap();
    let root = run(doc! {"$replaceRoot": {"newRoot": "$a"}}, input).unwrap();
    assert_eq!(with, vec![doc! {"x": 1}, doc! {"x": 2}]);
    assert_eq!(with, root);
}

#[test]
fn replace_with_takes_an_expression_document_as_the_new_root() {
    let out = run(
        doc! {"$replaceWith": {"id": "$_id", "label": {"$concat": ["n", "$s"]}}},
        vec![doc! {"_id": 7, "s": "4"}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"id": 7, "label": "n4"}]);
    // `$mergeObjects` is the usual way to fold a field into the document.
    let out = run(
        doc! {"$replaceWith": {"$mergeObjects": [{"d": 0}, "$a"]}},
        vec![doc! {"a": {"d": 1, "e": 2}}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"d": 1, "e": 2}]);
}

#[test]
fn replace_with_refuses_a_root_that_is_not_a_document_as_replace_root_does() {
    for new_root in ["$missing", "$n", "$s"] {
        let input = vec![doc! {"n": 1, "s": "text"}];
        let with = run(doc! {"$replaceWith": new_root}, input.clone()).unwrap_err().to_string();
        let root =
            run(doc! {"$replaceRoot": {"newRoot": new_root}}, input).unwrap_err().to_string();
        assert!(with.contains("$replaceRoot needs a document"), "{with}");
        assert_eq!(with, root);
    }
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

fn project_out(paths: &[&str]) -> Document {
    let mut spec = Document::new();
    for p in paths {
        spec.insert(*p, 0);
    }
    doc! {"$project": spec}
}

#[test]
fn unset_removes_one_path_or_several() {
    let input = vec![doc! {"_id": 1, "a": 1, "b": 2, "c": {"d": 3, "e": 4}}];
    assert_eq!(
        run(doc! {"$unset": "a"}, input.clone()).unwrap(),
        vec![doc! {"_id": 1, "b": 2, "c": {"d": 3, "e": 4}}]
    );
    assert_eq!(
        run(doc! {"$unset": ["a", "c.d"]}, input.clone()).unwrap(),
        vec![doc! {"_id": 1, "b": 2, "c": {"e": 4}}]
    );
    // `_id` can be removed.
    assert_eq!(
        run(doc! {"$unset": ["_id", "b", "c"]}, input.clone()).unwrap(),
        vec![doc! {"a": 1}]
    );
    // A path that is not there is nothing to do.
    assert_eq!(run(doc! {"$unset": ["nope", "c.nope"]}, input.clone()).unwrap(), input);
}

#[test]
fn unset_goes_through_arrays_as_a_project_exclusion_does() {
    let input =
        vec![doc! {"_id": 1, "items": [{"sku": 1, "q": 5}, {"sku": 2}, 7, [{"sku": 3, "q": 1}]]}];
    let unset = run(doc! {"$unset": "items.q"}, input.clone()).unwrap();
    let project = run(project_out(&["items.q"]), input).unwrap();
    assert_eq!(unset, project);
    assert_eq!(unset, vec![doc! {"_id": 1, "items": [{"sku": 1}, {"sku": 2}, 7, [{"sku": 3}]]}]);
}

#[test]
fn unset_equals_a_project_exclusion_of_the_same_paths() {
    let input = vec![
        doc! {"_id": 1, "a": {"b": {"c": 1, "d": 2}}, "e": [1, 2], "f": Bson::Null},
        doc! {"_id": 2, "a": 5, "e": {"x": 1}},
    ];
    let paths = ["a.b.c", "e.x", "f"];
    assert_eq!(
        run(doc! {"$unset": paths.to_vec()}, input.clone()).unwrap(),
        run(project_out(&paths), input).unwrap()
    );
}

#[test]
fn unset_refuses_a_bad_operand() {
    for operand in [
        Bson::Array(vec![]),
        Bson::Array(vec![Bson::String("a".into()), Bson::Int32(1)]),
        Bson::Array(vec![Bson::Null]),
        Bson::Array(vec![Bson::Array(vec![])]),
        Bson::Int32(1),
        Bson::Null,
        Bson::Boolean(true),
        Bson::Document(doc! {"a": 0}),
    ] {
        let message = refused(doc! {"$unset": operand.clone()});
        assert!(message.contains("$unset"), "{operand:?}: {message}");
    }
}

#[test]
fn unset_refuses_a_bad_path() {
    for path in ["", "a..b", ".a", "a.", "$a", "a.$b", "$"] {
        let message = refused(doc! {"$unset": path});
        assert!(message.contains("$unset paths are field names"), "{path:?}: {message}");
        let message = refused(doc! {"$unset": [path]});
        assert!(message.contains("$unset paths are field names"), "{path:?}: {message}");
    }
}

#[test]
fn unset_refuses_the_same_field_twice() {
    for paths in [vec!["a", "a"], vec!["a", "a.b"], vec!["a.b", "a"], vec!["x", "a.b.c", "a.b"]] {
        let message = refused(doc! {"$unset": paths.clone()});
        assert!(message.contains("same field or one inside the other"), "{paths:?}: {message}");
    }
    // A shared prefix that is not a path prefix is fine.
    assert!(aggregate::parse(&[doc! {"$unset": ["a", "ab", "a_b", "a.b", ]}]).is_err());
    assert!(aggregate::parse(&[doc! {"$unset": ["ab", "a.b", "a_b"]}]).is_ok());
}

// ---------------------------------------------------------------------------
// $sample
// ---------------------------------------------------------------------------

fn numbered(n: i32) -> Vec<Document> {
    (0..n).map(|i| doc! {"_id": i}).collect()
}

fn size_of(stage: &Stage) -> usize {
    let Stage::Sample(n) = stage else { panic!("not a $sample: {stage:?}") };
    *n
}

/// A deterministic source of positions, for tests: a xorshift.
fn seeded(seed: u64) -> impl FnMut(usize) -> usize {
    let mut state = seed;
    move |n| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % n as u64) as usize
    }
}

#[test]
fn sample_parses_a_size_of_any_number_type_truncated_toward_zero() {
    for (size, want) in [
        (Bson::Int32(3), 3),
        (Bson::Int64(3), 3),
        (Bson::Double(3.0), 3),
        (Bson::Double(1.5), 1),
        (Bson::Double(2.999), 2),
        (Bson::Double(0.5), 0),
        (Bson::Double(-0.5), 0),
        (Bson::Int32(0), 0),
        (Bson::Decimal128("3".parse().unwrap()), 3),
        // A decimal rounds half to even, as the reference does.
        (Bson::Decimal128("2.5".parse().unwrap()), 2),
        (Bson::Decimal128("3.5".parse().unwrap()), 4),
        (Bson::Decimal128("2.9".parse().unwrap()), 3),
        (Bson::Decimal128("2.1".parse().unwrap()), 2),
        (Bson::Decimal128("0.5".parse().unwrap()), 0),
        // NaN is 0 documents; an enormous size is every document.
        (Bson::Double(f64::NAN), 0),
        (Bson::Decimal128("NaN".parse().unwrap()), 0),
    ] {
        let stages = aggregate::parse(&[doc! {"$sample": {"size": size.clone()}}]).unwrap();
        assert_eq!(size_of(&stages[0]), want, "{size:?}");
    }
}

#[test]
fn sample_of_size_zero_answers_nothing_and_of_a_fraction_that_many_documents() {
    // Reference 6.0.5: size 0 answers [], size 1.5 answers one document.
    assert!(run(doc! {"$sample": {"size": 0}}, numbered(5)).unwrap().is_empty());
    assert!(run(doc! {"$sample": {"size": 0.5}}, numbered(5)).unwrap().is_empty());
    assert_eq!(run(doc! {"$sample": {"size": 1.5}}, numbered(5)).unwrap().len(), 1);
    assert_eq!(run(doc! {"$sample": {"size": 2.0}}, numbered(5)).unwrap().len(), 2);
}

#[test]
fn sample_refuses_a_bad_size_in_the_references_words() {
    for (size, want) in [
        (Bson::Int32(-1), "size argument to $sample must not be negative"),
        (Bson::Int64(-5), "size argument to $sample must not be negative"),
        (Bson::Double(-1.0), "size argument to $sample must not be negative"),
        (Bson::Double(-1.5), "size argument to $sample must not be negative"),
        (Bson::String("3".into()), "size argument to $sample must be a number"),
        (Bson::Null, "size argument to $sample must be a number"),
        (Bson::Boolean(true), "size argument to $sample must be a number"),
        (Bson::Document(doc! {}), "size argument to $sample must be a number"),
        (Bson::Array(vec![Bson::Int32(3)]), "size argument to $sample must be a number"),
    ] {
        let message = refused(doc! {"$sample": {"size": size.clone()}});
        assert!(message.contains(want), "{size:?}: {message}");
    }
}

#[test]
fn an_enormous_sample_size_is_every_document_in_a_random_order() {
    // The reference answers every document for 1e6, 100001 and a decimal Infinity.
    for size in [
        Bson::Double(f64::INFINITY),
        Bson::Double(1e300),
        Bson::Int64(i64::MAX),
        Bson::Decimal128("Infinity".parse().unwrap()),
    ] {
        let out = run(doc! {"$sample": {"size": size.clone()}}, numbered(30)).unwrap();
        let mut ids: Vec<i64> = out.iter().map(|d| i64::from(d.get_i32("_id").unwrap())).collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..30).collect::<Vec<_>>(), "{size:?}");
    }
}

#[test]
fn sample_refuses_a_missing_size_an_extra_field_and_a_non_document() {
    assert!(refused(doc! {"$sample": {}}).contains("$sample stage must specify a size"));
    assert!(
        refused(doc! {"$sample": {"size": 3, "seed": 1}})
            .contains("unrecognized option to $sample: seed")
    );
    assert!(
        refused(doc! {"$sample": {"seed": 1}}).contains("unrecognized option to $sample: seed")
    );
    assert!(refused(doc! {"$sample": 3}).contains("$sample takes a document"));
    assert!(refused(doc! {"$sample": Bson::Null}).contains("$sample takes a document"));
}

#[test]
fn sample_of_a_size_that_covers_the_input_is_the_whole_input_in_a_random_order() {
    let input = numbered(5);
    for size in [5, 6, 100] {
        let out = stages::sample(input.clone(), size, &Limits::default(), &mut seeded(9)).unwrap();
        let mut sorted = out.clone();
        sorted.sort_by_key(|d| d.get_i32("_id").unwrap());
        assert_eq!(sorted, input, "size {size}: every document once");
    }
    // Through the stage with the system's source: not the input order, run after run (a
    // permutation of 20 documents is the input order once in 20!).
    let input = numbered(20);
    for _ in 0..3 {
        let out = run(doc! {"$sample": {"size": 100}}, input.clone()).unwrap();
        assert_eq!(out.len(), 20);
        assert_ne!(out, input);
    }
}

#[test]
fn sample_of_nothing_is_nothing() {
    assert!(run(doc! {"$sample": {"size": 3}}, vec![]).unwrap().is_empty());
}

#[test]
fn sample_picks_the_size_asked_each_document_at_most_once() {
    for seed in 1..40 {
        let out = stages::sample(numbered(50), 7, &Limits::default(), &mut seeded(seed)).unwrap();
        assert_eq!(out.len(), 7);
        let mut ids: Vec<i32> = out.iter().map(|d| d.get_i32("_id").unwrap()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 7, "seed {seed}: a document twice");
        assert!(ids.iter().all(|i| (0..50).contains(i)));
    }
}

#[test]
fn sample_is_a_function_of_its_source_of_positions() {
    let a = stages::sample(numbered(30), 5, &Limits::default(), &mut seeded(3)).unwrap();
    let b = stages::sample(numbered(30), 5, &Limits::default(), &mut seeded(3)).unwrap();
    assert_eq!(a, b);
    let c = stages::sample(numbered(30), 5, &Limits::default(), &mut seeded(4)).unwrap();
    assert_ne!(a, c);
}

#[test]
fn sample_replaces_a_slot_exactly_as_algorithm_r_does() {
    // Size 2 over 0..5. The first two fill slots 0 and 1. Document 2 draws below(3), 3
    // draws below(4), 4 draws below(5); a draw under 2 replaces that slot.
    let mut draws = vec![1usize, 5 - 1, 0].into_iter();
    let mut asked = Vec::new();
    // The three draws of the reservoir, then the shuffle's one (`below(2)`), which this
    // script answers with the last position, leaving the order as it is.
    let mut below = |n: usize| {
        asked.push(n);
        draws.next().unwrap_or(n - 1)
    };
    let out = stages::sample(numbered(5), 2, &Limits::default(), &mut below).unwrap();
    // 2 -> slot 1; 3 -> 3 is not below 2, skipped; 4 -> slot 0.
    assert_eq!(out, vec![doc! {"_id": 4}, doc! {"_id": 2}]);
    assert_eq!(asked, [3, 4, 5, 2]);
}

#[test]
fn sample_chooses_every_document_with_the_same_chance() {
    // 20000 samples of 3 from 10: each document is expected in 30% of them, 6000 times,
    // with a standard deviation of about 65. Generous bounds, 5000 to 7000, are 15 sigma.
    let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(0x5eed);
    let mut below = |n: usize| rand::RngExt::random_range(&mut rng, 0..n);
    let mut hits = [0usize; 10];
    for _ in 0..20_000 {
        let out = stages::sample(numbered(10), 3, &Limits::default(), &mut below).unwrap();
        for d in out {
            hits[d.get_i32("_id").unwrap() as usize] += 1;
        }
    }
    for (id, n) in hits.iter().enumerate() {
        assert!((5000..=7000).contains(n), "document {id} was chosen {n} times: {hits:?}");
    }
}

#[test]
fn the_systems_source_gives_different_samples() {
    // 1 in C(40,5) = 658008 that two runs agree; three runs in a row all agreeing is not
    // a thing that happens.
    let runs: Vec<Vec<Document>> =
        (0..3).map(|_| run(doc! {"$sample": {"size": 5}}, numbered(40)).unwrap()).collect();
    assert!(runs.iter().all(|r| r.len() == 5));
    assert!(runs[0] != runs[1] || runs[1] != runs[2], "{runs:?}");
}

#[test]
fn sample_orders_its_answer_at_random_so_a_limit_after_it_reaches_every_document() {
    // The reference: `[{$sample: {size: 3}}, {$limit: 1}]` over 10 documents answers each
    // one a tenth of the time. 20000 runs: 2000 expected each, a standard deviation of
    // 42; bounds 1700 to 2300 are about 7 sigma.
    let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(0x0dd5);
    let mut below = |n: usize| rand::RngExt::random_range(&mut rng, 0..n);
    let mut first = [0usize; 10];
    let mut ascending = 0usize;
    for _ in 0..20_000 {
        let out = stages::sample(numbered(10), 3, &Limits::default(), &mut below).unwrap();
        let ids: Vec<i32> = out.iter().map(|d| d.get_i32("_id").unwrap()).collect();
        first[ids[0] as usize] += 1;
        ascending += usize::from(ids.windows(2).all(|w| w[0] < w[1]));
    }
    for (id, n) in first.iter().enumerate() {
        assert!((1700..=2300).contains(n), "document {id} came first {n} times: {first:?}");
    }
    // Three chosen documents are in ascending order one time in six (3333 expected, a
    // standard deviation of 52), not the 21% a reservoir's slot order gives.
    assert!((3000..=3700).contains(&ascending), "{ascending} ascending");
    // And a size that covers the input is a shuffle: every document first about equally.
    let mut first = [0usize; 5];
    for _ in 0..10_000 {
        let out = stages::sample(numbered(5), 9, &Limits::default(), &mut below).unwrap();
        first[out[0].get_i32("_id").unwrap() as usize] += 1;
    }
    for (id, n) in first.iter().enumerate() {
        assert!((1700..=2300).contains(n), "document {id} came first {n} times: {first:?}");
    }
}

#[test]
fn sample_has_no_byte_cap_of_its_own() {
    // The input is documents the pipeline already holds (held to the document limit); the
    // reservoir moves some of them and holds none more. Documents of 5 MiB, four of them.
    let pad = "x".repeat(5 * 1024 * 1024);
    let big: Vec<Document> = (0..4).map(|i| doc! {"_id": i, "pad": pad.clone()}).collect();
    let out = stages::sample(big, 4, &Limits::default(), &mut seeded(1)).unwrap();
    assert_eq!(out.len(), 4);
}

// ---------------------------------------------------------------------------
// $redact
// ---------------------------------------------------------------------------

fn redact(expr: Bson, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    run(doc! {"$redact": expr}, input)
}

/// `{$cond: [{$eq: ["$level", n]}, then, else]}`.
fn level_is(n: i32, then: &str, otherwise: &str) -> Bson {
    Bson::Document(doc! {"$cond": [{"$eq": ["$level", n]}, then, otherwise]})
}

#[test]
fn redact_prunes_a_level_and_descends_through_the_rest() {
    let input = vec![doc! {
        "_id": 1, "level": 1, "title": "t",
        "cc": {"level": 5, "num": 1234, "billing": {"level": 5, "city": "NY"}},
        "ship": [{"level": 3, "city": "A"}, {"level": 5, "city": "B"}, 9, {"level": 1, "city": "C"}],
        "meta": {"level": 2, "tags": ["x", {"level": 5, "z": 1}, ["y", {"level": 5}]]},
    }];
    let out = redact(level_is(5, "$$PRUNE", "$$DESCEND"), input).unwrap();
    assert_eq!(
        out,
        vec![doc! {
            "_id": 1, "level": 1, "title": "t",
            "ship": [{"level": 3, "city": "A"}, 9, {"level": 1, "city": "C"}],
            "meta": {"level": 2, "tags": ["x", ["y"]]},
        }]
    );
}

#[test]
fn redact_prunes_a_whole_top_level_document() {
    let input = vec![
        doc! {"_id": 1, "level": 5, "a": 1},
        doc! {"_id": 2, "level": 1, "a": 2},
        doc! {"_id": 3, "level": 5, "a": 3},
    ];
    let out = redact(level_is(5, "$$PRUNE", "$$KEEP"), input).unwrap();
    assert_eq!(out, vec![doc! {"_id": 2, "level": 1, "a": 2}]);
    // All pruned is an empty stream.
    let out = redact(Bson::String("$$PRUNE".into()), vec![doc! {"_id": 1}]).unwrap();
    assert!(out.is_empty());
}

#[test]
fn redact_keep_keeps_everything_below_without_looking() {
    // The embedded document would be pruned if it were looked at; KEEP at the top ends it.
    let input = vec![doc! {"_id": 1, "level": 1, "sub": {"level": 5, "secret": 1}}];
    let out = redact(level_is(1, "$$KEEP", "$$PRUNE"), input.clone()).unwrap();
    assert_eq!(out, input);
    // A bare `$$KEEP` keeps every document whole.
    assert_eq!(redact(Bson::String("$$KEEP".into()), input.clone()).unwrap(), input);
}

#[test]
fn redact_descend_keeps_a_level_that_has_no_documents_below() {
    let input = vec![doc! {"_id": 1, "a": 1, "b": [1, 2, 3], "c": "s", "d": Bson::Null}];
    assert_eq!(redact(Bson::String("$$DESCEND".into()), input.clone()).unwrap(), input);
}

#[test]
fn redact_leaves_an_array_that_lost_all_its_documents_empty() {
    let input = vec![
        doc! {"_id": 1, "level": 1, "xs": [{"level": 5}, {"level": 5}], "ys": [[{"level": 5}]]},
    ];
    let out = redact(level_is(5, "$$PRUNE", "$$DESCEND"), input).unwrap();
    assert_eq!(out, vec![doc! {"_id": 1, "level": 1, "xs": [], "ys": [[]]}]);
}

#[test]
fn redact_reads_fields_from_the_level_and_root_from_the_top() {
    // `$$ROOT.owner` is always the top document's; `$user` is the level's own. A level
    // with no `user` descends; one whose user is the owner descends; any other is pruned.
    let expr = Bson::Document(doc! {"$cond": [
        {"$or": [
            {"$eq": [{"$type": "$user"}, "missing"]},
            {"$eq": ["$user", "$$ROOT.owner"]},
        ]},
        "$$DESCEND",
        "$$PRUNE",
    ]});
    let input = vec![doc! {
        "_id": 1, "owner": "ann",
        "mine": {"user": "ann", "x": 1, "inner": {"user": "bob", "y": 2}},
        "theirs": {"user": "bob", "x": 2},
        "open": {"x": 3},
    }];
    let out = redact(expr, input).unwrap();
    assert_eq!(
        out,
        vec![doc! {"_id": 1, "owner": "ann", "mine": {"user": "ann", "x": 1}, "open": {"x": 3}}]
    );
}

#[test]
fn redact_current_is_the_level_and_root_is_the_top_document() {
    // `$$CURRENT.tag` is the level's own tag, `$$ROOT.tag` the top document's: prune a level
    // whose tag differs from the top's.
    let expr = Bson::Document(doc! {"$cond": [
    {"$eq": ["$$CURRENT.tag", "$$ROOT.tag"]}, "$$DESCEND", "$$PRUNE"]});
    let input =
        vec![doc! {"tag": "a", "same": {"tag": "a", "n": 1}, "other": {"tag": "b", "n": 2}}];
    let out = redact(expr, input).unwrap();
    assert_eq!(out, vec![doc! {"tag": "a", "same": {"tag": "a", "n": 1}}]);
}

#[test]
fn redact_works_with_set_operators() {
    // The classic access-control shape: keep what shares a tag with the caller's.
    let expr = Bson::Document(doc! {"$cond": {
        "if": {"$gt": [{"$size": {"$setIntersection": ["$tags", ["STLW", "G"]]}}, 0]},
        "then": "$$DESCEND",
        "else": "$$PRUNE",
    }});
    let input = vec![doc! {
        "_id": 1, "tags": ["G", "STLW"], "x": 1,
        "sub": {"tags": ["G", "STLW"], "y": 2},
        "hidden": {"tags": ["TK"], "z": 3},
    }];
    let out = redact(expr, input).unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 1, "tags": ["G", "STLW"], "x": 1, "sub": {"tags": ["G", "STLW"], "y": 2}}
        ]
    );

    let any = Bson::Document(doc! {"$cond": [
    {"$anyElementTrue": [{"$map": {"input": "$tags", "as": "t", "in": {"$in": ["$$t", ["A", "B"]]}}}]},
    "$$KEEP", "$$PRUNE"]});
    let input = vec![doc! {"_id": 1, "tags": ["B", "Q"]}, doc! {"_id": 2, "tags": ["Q"]}];
    assert_eq!(redact(any, input).unwrap(), vec![doc! {"_id": 1, "tags": ["B", "Q"]}]);
}

#[test]
fn redact_answers_other_than_the_three_are_an_error() {
    for answer in [
        Bson::Int32(1),
        Bson::Null,
        Bson::String("other".into()),
        Bson::Boolean(true),
        Bson::Document(doc! {"a": 1}),
        Bson::String("$missing".into()),
    ] {
        let err = redact(answer.clone(), vec![doc! {"_id": 1}]).expect_err("must be refused");
        assert!(err.to_string().contains("$redact's expression must answer"), "{answer:?}: {err}");
    }
    // The error comes from an embedded level just as from the top.
    let expr =
        Bson::Document(doc! {"$cond": [{"$eq": [{"$type": "$bad"}, "missing"]}, "$$DESCEND", 7]});
    let err = redact(expr, vec![doc! {"_id": 1, "sub": {"bad": 1}}]).expect_err("level two");
    assert!(err.to_string().contains("found an int"), "{err}");
}

#[test]
fn redact_of_nothing_is_nothing() {
    assert!(redact(Bson::String("$$KEEP".into()), vec![]).unwrap().is_empty());
}

#[test]
fn redact_variables_exist_only_inside_redact() {
    for name in ["DESCEND", "PRUNE", "KEEP"] {
        let var = format!("$${name}");
        for stage in [
            doc! {"$project": {"x": var.clone()}},
            doc! {"$addFields": {"x": var.clone()}},
            doc! {"$match": {"$expr": {"$eq": ["$a", var.clone()]}}},
            doc! {"$replaceWith": {"x": var.clone()}},
            doc! {"$group": {"_id": var.clone()}},
            doc! {"$sortByCount": var.clone()},
        ] {
            let message = refused(stage.clone());
            assert!(
                message.contains(&var) && message.contains("valid only inside a $redact"),
                "{stage}: {message}"
            );
        }
    }
    // The other names stay refused, inside $redact as outside it.
    for name in ["CLUSTER_TIME", "USER_ROLES", "SEARCH_META", "NOPE"] {
        let var = format!("$${name}");
        for stage in [
            doc! {"$redact": {"$cond": [{"$eq": ["$a", var.clone()]}, "$$KEEP", "$$PRUNE"]}},
            doc! {"$project": {"x": var.clone()}},
        ] {
            let message = refused(stage.clone());
            assert!(
                message.contains(&var) && message.contains("does not have"),
                "{stage}: {message}"
            );
        }
    }
    // They carry no fields.
    let message = refused(doc! {"$redact": "$$KEEP.a"});
    assert!(message.contains("has no fields"), "{message}");
}

#[test]
fn redact_variables_read_inside_nested_expressions_of_the_redact() {
    // In a $switch, a $let body and a $map, still inside $redact.
    let expr = Bson::Document(doc! {"$switch": {
        "branches": [
            {"case": {"$eq": ["$level", 0]}, "then": "$$KEEP"},
            {"case": {"$eq": ["$level", 9]}, "then": "$$PRUNE"},
        ],
        "default": {"$let": {"vars": {"d": "$$DESCEND"}, "in": "$$d"}},
    }});
    let input = vec![doc! {"level": 1, "a": {"level": 0, "b": {"level": 9}}, "c": {"level": 9}}];
    let out = redact(expr, input).unwrap();
    assert_eq!(out, vec![doc! {"level": 1, "a": {"level": 0, "b": {"level": 9}}}]);
}

#[test]
fn a_redact_stage_parses_in_a_facet_and_a_lookup_sub_pipeline() {
    let facet = aggregate::parse(&[doc! {"$facet": {"f": [{"$redact": "$$KEEP"}]}}]);
    assert!(facet.is_ok(), "{facet:?}");
    let lookup = aggregate::parse(&[doc! {"$lookup": {
    "from": "x", "as": "j", "pipeline": [{"$redact": "$$PRUNE"}]}}]);
    assert!(lookup.is_ok(), "{lookup:?}");
}

#[test]
fn redact_run_inside_a_facet_sees_the_facets_input() {
    let stages = aggregate::parse(&[doc! {"$facet": {
        "kept": [{"$redact": {"$cond": [{"$eq": ["$level", 5]}, "$$PRUNE", "$$DESCEND"]}}, {"$sortByCount": "$level"}],
        "sampled": [{"$sample": {"size": 2}}, {"$count": "n"}],
        "unset": [{"$unset": "level"}, {"$replaceWith": {"id": "$_id"}}],
    }}])
    .unwrap();
    let input =
        vec![doc! {"_id": 1, "level": 5}, doc! {"_id": 2, "level": 1}, doc! {"_id": 3, "level": 1}];
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    assert_eq!(
        out,
        vec![doc! {
            "kept": [{"_id": 1, "count": 2}],
            "sampled": [{"n": 2}],
            "unset": [{"id": 1}, {"id": 2}, {"id": 3}],
        }]
    );
}

// ---------------------------------------------------------------------------
// $unionWith (the parse; the run is the caller's, see kimmy-api)
// ---------------------------------------------------------------------------

fn union_of(stage: &Stage) -> (&str, usize) {
    let Stage::UnionWith(spec) = stage else { panic!("not a $unionWith: {stage:?}") };
    (&spec.coll, spec.stages.len())
}

#[test]
fn union_with_parses_both_forms() {
    let stages = aggregate::parse(&[
        doc! {"$unionWith": "other"},
        doc! {"$unionWith": {"coll": "more"}},
        doc! {"$unionWith": {"coll": "third", "pipeline": [{"$match": {"a": 1}}, {"$project": {"a": 1}}]}},
        doc! {"$unionWith": {"coll": "fourth", "pipeline": []}},
    ])
    .unwrap();
    assert_eq!(union_of(&stages[0]), ("other", 0));
    assert_eq!(union_of(&stages[1]), ("more", 0));
    assert_eq!(union_of(&stages[2]), ("third", 2));
    assert_eq!(union_of(&stages[3]), ("fourth", 0));
    assert!(stages.iter().all(Stage::needs_storage));
    assert_eq!(stages[0].name(), "$unionWith");
}

#[test]
fn union_with_refuses_what_it_cannot_honour() {
    let cases: Vec<(Bson, &str)> = vec![
        (Bson::Document(doc! {"pipeline": [{"$match": {}}]}), "needs `coll`"),
        (Bson::Document(doc! {}), "needs `coll`"),
        (Bson::Document(doc! {"coll": "a", "extra": 1}), "does not accept `extra`"),
        (Bson::Document(doc! {"coll": "a", "pipeline": {"$match": {}}}), "`pipeline` is an array"),
        (Bson::Document(doc! {"coll": "a", "pipeline": Bson::Null}), "`pipeline` is an array"),
        (Bson::Document(doc! {"coll": "a", "pipeline": ["x"]}), "each $unionWith pipeline stage"),
        (Bson::Document(doc! {"coll": 5}), "`coll` is a collection name"),
        (Bson::Document(doc! {"coll": ""}), "non-empty collection name"),
        (Bson::String(String::new()), "non-empty collection name"),
        (Bson::Int32(1), "$unionWith takes a collection name"),
        (Bson::Null, "$unionWith takes a collection name"),
        (Bson::Array(vec![]), "$unionWith takes a collection name"),
    ];
    for (operand, want) in cases {
        let message = refused(doc! {"$unionWith": operand.clone()});
        assert!(message.contains(want), "{operand:?}: {message}");
    }
}

#[test]
fn union_with_sub_pipeline_refuses_out_merge_and_vector_search_at_any_depth() {
    let wrap = |stage: Document| doc! {"$unionWith": {"coll": "a", "pipeline": [stage]}};
    for (stage, name) in [
        (doc! {"$out": "t"}, "$out"),
        (doc! {"$merge": {"into": "t"}}, "$merge"),
        (doc! {"$vectorSearch": {"query": "q"}}, "$vectorSearch"),
    ] {
        let message = refused(wrap(stage.clone()));
        assert!(message.contains(name) && message.contains("$unionWith"), "{name}: {message}");
        // Under a nested $unionWith, a $lookup sub-pipeline of it, and a facet's.
        let nested = doc! {"$unionWith": {"coll": "a", "pipeline": [wrap(stage.clone())]}};
        assert!(aggregate::parse(&[nested]).is_err(), "{name} nested");
        let in_lookup = doc! {"$unionWith": {"coll": "a", "pipeline": [
        {"$lookup": {"from": "b", "as": "j", "pipeline": [stage.clone()]}}]}};
        assert!(aggregate::parse(&[in_lookup]).is_err(), "{name} in a lookup of it");
    }
    // Everything else is allowed in it, a nested $unionWith and a $lookup included.
    let ok = aggregate::parse(&[doc! {"$unionWith": {"coll": "a", "pipeline": [
        {"$match": {"a": 1}},
        {"$lookup": {"from": "b", "localField": "k", "foreignField": "k", "as": "j"}},
        {"$unionWith": "c"},
        {"$sample": {"size": 1}},
        {"$redact": "$$KEEP"},
        {"$sortByCount": "$k"},
    ]}}]);
    assert!(ok.is_ok(), "{ok:?}");
}

#[test]
fn union_with_sub_pipeline_accepts_a_facet_as_the_reference_does() {
    let ok = aggregate::parse(&[doc! {"$unionWith": {"coll": "a", "pipeline": [
        {"$facet": {"f": [{"$match": {"n": 1}}, {"$count": "n"}], "g": [{"$sortByCount": "$k"}]}},
    ]}}]);
    assert!(ok.is_ok(), "{ok:?}");
    // And inside a nested union and a $lookup of it.
    let nested = aggregate::parse(&[doc! {"$unionWith": {"coll": "a", "pipeline": [
        {"$unionWith": {"coll": "b", "pipeline": [{"$facet": {"f": []}}]}},
        {"$lookup": {"from": "c", "as": "j", "pipeline": [{"$facet": {"f": []}}]}},
    ]}}]);
    assert!(nested.is_ok(), "{nested:?}");
    // A $facet inside a $facet is still refused, a union between them or not.
    let message = refused(doc! {"$facet": {"f": [{"$unionWith": {"coll": "a", "pipeline": [
    {"$facet": {"g": []}}]}}]}});
    assert!(message.contains("$facet cannot be used inside a $facet"), "{message}");
}

#[test]
fn union_with_is_allowed_in_a_facet_and_a_lookup_sub_pipeline() {
    let stages = aggregate::parse(&[
        doc! {"$facet": {"f": [{"$unionWith": "a"}]}},
        doc! {"$lookup": {"from": "b", "as": "j", "pipeline": [{"$unionWith": "c"}]}},
    ])
    .unwrap();
    assert!(stages.iter().all(Stage::needs_storage));
    assert_eq!(aggregate::foreign_collections(&stages), ["a", "b", "c"]);
}

#[test]
fn union_with_collections_are_found_at_every_depth() {
    let stages = aggregate::parse(&[
        doc! {"$unionWith": {"coll": "a", "pipeline": [
            {"$unionWith": {"coll": "b", "pipeline": [{"$lookup": {"from": "c", "as": "j", "pipeline": []}}]}},
        ]}},
        doc! {"$unionWith": "a"},
    ])
    .unwrap();
    assert_eq!(aggregate::foreign_collections(&stages), ["a", "b", "c"]);
}

#[test]
fn the_pure_pipeline_refuses_union_with_rather_than_passing_it_through() {
    let err =
        run(doc! {"$unionWith": "a"}, vec![doc! {"_id": 1}]).expect_err("needs a storage handle");
    assert!(matches!(&err, Error::Unsupported(m) if m.contains("$unionWith")), "{err:?}");
}

#[test]
fn a_union_with_sub_pipeline_reads_the_enclosing_lets_names() {
    let ok = aggregate::parse(&[doc! {"$lookup": {
    "from": "x", "let": {"v": "$v"}, "as": "j",
    "pipeline": [{"$unionWith": {"coll": "y", "pipeline": [
        {"$match": {"$expr": {"$eq": ["$k", "$$v"]}}}]}}]}}]);
    assert!(ok.is_ok(), "{ok:?}");
    // Outside a `$lookup` nothing binds `$$v`.
    let outside = refused(doc! {"$unionWith": {"coll": "y", "pipeline": [
    {"$match": {"$expr": {"$eq": ["$k", "$$v"]}}}]}});
    assert!(outside.contains("v"), "{outside}");
}

#[test]
fn the_stage_list_in_the_unknown_stage_message_names_the_new_stages() {
    let message = refused(doc! {"$nope": 1});
    for stage in ["$replaceWith", "$unset", "$sortByCount", "$sample", "$redact", "$unionWith"] {
        assert!(message.contains(stage), "{stage} missing from: {message}");
    }
}

// ---------------------------------------------------------------------------
// $cond's named form, which a $redact expression is usually written in
// ---------------------------------------------------------------------------

#[test]
fn cond_takes_its_named_form() {
    let out = run(
        doc! {"$project": {"v": {"$cond": {"if": {"$gt": ["$n", 1]}, "then": "big", "else": "small"}}}},
        vec![doc! {"_id": 1, "n": 2}, doc! {"_id": 2, "n": 1}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": 1, "v": "big"}, doc! {"_id": 2, "v": "small"}]);
}

#[test]
fn cond_named_form_needs_all_three_and_no_others() {
    for spec in [
        doc! {"if": true, "then": 1},
        doc! {"then": 1, "else": 2},
        doc! {"if": true, "else": 2},
        doc! {"if": true, "then": 1, "else": 2, "extra": 3},
        doc! {},
    ] {
        let err = refused(doc! {"$project": {"v": {"$cond": spec.clone()}}});
        assert!(err.contains("$cond"), "{spec}: {err}");
    }
}
