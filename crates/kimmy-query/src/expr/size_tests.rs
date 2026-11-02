//! The size limits of the expression layer: a string, an array or a document an
//! expression builds is at most 16 MiB, refused before it is built, with a `413`
//! that names what hit which limit. The limits are parameters of the `*_within` functions, so
//! a test reaches them with a few bytes; the operators that read the real limit
//! are tested with values a megabyte wide.

use bson::{Bson, Document, doc};
use kimmy_core::{Error, Result};

use super::docops::{merge_objects_within, set_field_within};
use super::sizes::{ArrayBudget, bson_bytes, check_array_within};
use super::{
    Expr, MAX_ARRAY_BYTES, Scope, array_to_object_within, concat_arrays_within, eval_object,
    merge_budget, merge_into, object_to_array_within, range_bytes, range_within, split_within,
};

fn ev(expr: Document, d: &Document) -> Result<Bson> {
    Expr::parse(&Bson::Document(expr))?.eval(d)
}

fn s(v: &str) -> Bson {
    Bson::String(v.into())
}

fn ints(n: usize) -> Bson {
    Bson::Array((0..n).map(|i| Bson::Int32(i as i32)).collect())
}

/// The bytes and the limit of a refusal, or a panic naming what came instead.
fn refusal(got: Result<Bson>) -> (usize, usize, String) {
    match got {
        Err(err @ Error::ValueTooLarge { bytes, max, .. }) => (bytes, max, err.to_string()),
        other => panic!("expected ValueTooLarge, got {other:?}"),
    }
}

/// How many bytes `value` takes as the element of a document: the framing of
/// `{"a": value}` is a header, a type byte, `a` and its terminator, and the
/// document's terminator, which come off.
fn encoded(value: &Bson) -> usize {
    bson::serialize_to_vec(&doc! {"a": value.clone()}).unwrap().len() - (4 + 1 + 2 + 1)
}

#[test]
fn bson_bytes_is_the_size_the_value_encodes_to() {
    let values = [
        Bson::Double(1.5),
        Bson::Int32(1),
        Bson::Int64(1),
        Bson::Boolean(true),
        Bson::Null,
        Bson::String("héllo".into()),
        Bson::Decimal128("1.5".parse().unwrap()),
        Bson::DateTime(bson::DateTime::from_millis(1)),
        Bson::Timestamp(bson::Timestamp { time: 1, increment: 2 }),
        Bson::ObjectId(bson::oid::ObjectId::new()),
        Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![1; 9],
        }),
        Bson::RegularExpression(bson::Regex {
            pattern: "ab+".to_string().try_into().unwrap(),
            options: "i".to_string().try_into().unwrap(),
        }),
        Bson::MinKey,
        Bson::MaxKey,
        Bson::Document(doc! {}),
        Bson::Document(doc! {"x": 1, "yy": "z", "n": {"k": [1, 2.5, "s"]}}),
        Bson::Array(vec![]),
        ints(11),
        Bson::Array(vec![s("a"), Bson::Array(vec![Bson::Null; 12]), Bson::Document(doc! {"q": 1})]),
    ];
    for v in &values {
        assert_eq!(bson_bytes(v), encoded(v), "{v:?}");
    }
    // 120 elements: the index grows to three digits.
    assert_eq!(bson_bytes(&ints(120)), encoded(&ints(120)));
}

#[test]
fn an_array_budget_is_exact_at_its_limit() {
    let array = ints(25);
    let Bson::Array(items) = &array else { unreachable!() };
    let size = bson_bytes(&array);
    assert!(check_array_within("the array x builds", items, size).is_ok());
    let (bytes, max, text) =
        refusal(check_array_within("the array x builds", items, size - 1).map(|()| Bson::Null));
    assert_eq!((bytes, max), (size, size - 1));
    assert_eq!(
        text,
        format!(
            "the array x builds is {size} bytes, over the 16 MiB array limit; build a smaller \
             array, or $slice, $filter or $limit the input"
        )
    );
    // It refuses at the element that passes, not after building them all.
    let mut budget = ArrayBudget::new("the array x builds", 5 + 3 * 7);
    for i in 0..3 {
        budget.add(&Bson::Int32(i)).unwrap();
    }
    assert!(budget.add(&Bson::Int32(3)).is_err());
}

#[test]
fn concat_arrays_counts_before_it_copies() {
    let a = ints(2);
    let b = ints(1);
    let all = Bson::Array(vec![Bson::Int32(0), Bson::Int32(1), Bson::Int32(0)]);
    let size = bson_bytes(&all);
    assert_eq!(concat_arrays_within(&[a.clone(), b.clone()], size).unwrap(), all);
    let (bytes, max, text) = refusal(concat_arrays_within(&[a.clone(), b.clone()], size - 1));
    assert_eq!((bytes, max), (size, size - 1));
    assert!(text.starts_with("the array $concatArrays builds is "), "{text}");
    // A null makes the result null whatever the sizes; a non-array is a type error.
    assert_eq!(concat_arrays_within(&[a.clone(), Bson::Null], 0).unwrap(), Bson::Null);
    let err = concat_arrays_within(&[a, Bson::Int32(1)], 0).unwrap_err();
    assert!(matches!(err, Error::InvalidQuery(_)), "{err:?}");
    assert_eq!(concat_arrays_within(&[], 5).unwrap(), Bson::Array(vec![]));
}

#[test]
fn a_reduce_that_doubles_an_array_stops_at_16_mib() {
    // One element of a megabyte: 8 copies are 8 MiB, 16 are over 16 MiB.
    let d = doc! {"big": "x".repeat(1 << 20)};
    let double = |steps: usize| {
        ev(
            doc! {"$reduce": {
                "input": vec![Bson::Int32(0); steps],
                "initialValue": ["$big"],
                "in": {"$concatArrays": ["$$value", "$$value"]},
            }},
            &d,
        )
    };
    let Bson::Array(eight) = double(3).unwrap() else { panic!("an array") };
    assert_eq!(eight.len(), 8);
    drop(eight);
    let (bytes, max, text) = refusal(double(4));
    assert_eq!(max, MAX_ARRAY_BYTES);
    assert!(bytes > MAX_ARRAY_BYTES && bytes < 17 * (1 << 20) + 1024, "{bytes}");
    assert!(
        text.contains("the array $concatArrays builds is") && text.contains("16 MiB array limit")
    );
}

#[test]
fn map_zip_and_an_array_literal_count_the_values_they_collect() {
    let d = doc! {"big": "x".repeat(1 << 20)};
    // Twenty elements of a megabyte each.
    let twenty = vec![Bson::Int32(0); 20];
    let (_, _, text) = refusal(ev(doc! {"$map": {"input": twenty.clone(), "in": "$big"}}, &d));
    assert!(text.starts_with("the array $map builds is "), "{text}");
    let fifteen = vec![Bson::Int32(0); 15];
    assert!(ev(doc! {"$map": {"input": fifteen, "in": "$big"}}, &d).is_ok());

    // Two inputs of fifteen megabyte strings each are within the limit alone; their
    // rows together are thirty.
    let fifteen_refs = || Bson::Array(vec![s("$big"); 15]);
    assert!(ev(doc! {"$size": [fifteen_refs()]}, &d).is_ok());
    let (_, _, text) = refusal(ev(doc! {"$zip": {"inputs": [fifteen_refs(), fifteen_refs()]}}, &d));
    assert!(text.starts_with("the array $zip builds is "), "{text}");
    assert!(ev(doc! {"$zip": {"inputs": [fifteen_refs(), vec![Bson::Int32(1); 15]]}}, &d).is_ok());

    // An array literal of twenty: refused as it is collected.
    let (_, _, text) = refusal(ev(doc! {"$size": [Bson::Array(vec![s("$big"); 20])]}, &d));
    assert!(text.starts_with("the array literal is "), "{text}");
}

#[test]
fn split_counts_the_pieces_as_it_cuts_them() {
    // "a,b,c" is three pieces of one byte: 5 bytes of framing and 9 + 9 + 9.
    let ok = split_within("a,b,c", ",", 5 + 3 * 9).unwrap();
    assert_eq!(ok, Bson::Array(vec![s("a"), s("b"), s("c")]));
    assert_eq!(bson_bytes(&ok), 5 + 3 * 9);
    let (bytes, max, text) = refusal(split_within("a,b,c", ",", 5 + 3 * 9 - 1));
    assert_eq!((bytes, max), (5 + 3 * 9, 5 + 3 * 9 - 1));
    assert!(text.starts_with("the array $split builds is "), "{text}");
    // A string of delimiters is an empty piece per byte: far more array than string.
    let commas = ",".repeat(10_000);
    let (bytes, _, _) = refusal(split_within(&commas, ",", 20_000));
    assert!(bytes > 20_000 && bytes < 20_000 + 20, "refused at the piece that passed: {bytes}");
}

#[test]
fn object_to_array_counts_each_pair_before_it_builds_it() {
    let fields = doc! {"a": 1, "bb": "x", "c": {"d": [1, 2]}};
    let Bson::Array(pairs) = object_to_array_within(&fields, usize::MAX).unwrap() else {
        panic!("an array")
    };
    let size = bson_bytes(&Bson::Array(pairs.clone()));
    assert_eq!(pairs.len(), 3);
    assert!(object_to_array_within(&fields, size).is_ok());
    let (bytes, max, text) = refusal(object_to_array_within(&fields, size - 1));
    assert_eq!((bytes, max), (size, size - 1));
    assert!(text.starts_with("the array $objectToArray builds is "), "{text}");
    // A document of small fields becomes an array several times its size.
    let small = doc! {"a": 1, "b": 2, "c": 3, "d": 4};
    assert!(size_of_pairs(&small) > 3 * bson_bytes(&Bson::Document(small.clone())) / 2);
}

fn size_of_pairs(fields: &Document) -> usize {
    let Bson::Array(pairs) = object_to_array_within(fields, usize::MAX).unwrap() else {
        panic!("an array")
    };
    bson_bytes(&Bson::Array(pairs))
}

#[test]
fn range_is_bounded_before_it_allocates() {
    // A span of two billion is refused by its size, not built: the count and its
    // bytes are worked out first. A 413, as every other builder's refusal.
    for args in [
        vec![Bson::Int32(0), Bson::Int32(i32::MAX)],
        vec![Bson::Int64(0), Bson::Int64(i64::from(i32::MAX))],
        vec![Bson::Int64(i64::from(i32::MIN)), Bson::Int64(i64::from(i32::MAX)), Bson::Int32(1)],
        vec![Bson::Int32(0), Bson::Int32(i32::MAX), Bson::Int32(2)],
        vec![Bson::Int32(0), Bson::Int32(1_000_000_000)],
    ] {
        let (bytes, max, text) = refusal(ev(doc! {"$range": args}, &doc! {}));
        assert_eq!(max, MAX_ARRAY_BYTES);
        assert!(bytes > MAX_ARRAY_BYTES, "{bytes}");
        assert!(text.starts_with("the array $range builds is "), "{text}");
        assert!(text.contains("16 MiB array limit"), "{text}");
    }
    // A bound past `Int32` is refused as a 32-bit bound, as the reference refuses it (34446).
    let err = ev(doc! {"$range": [Bson::Int64(0), Bson::Int64(i64::MAX)]}, &doc! {}).unwrap_err();
    assert!(matches!(&err, Error::InvalidQuery(m) if m.contains("32-bit integer")), "{err:?}");
    // The count that is refused is a count of bytes, not of elements: the old cap of
    // 100,000 elements is gone, and the reference answers 200,000.
    for n in [100_000, 100_001, 200_000] {
        let got = ev(doc! {"$range": [0, n]}, &doc! {}).unwrap();
        assert_eq!(got.as_array().unwrap().len(), n as usize);
    }
    // A step skips elements, so a huge span with a huge step answers.
    let got = ev(doc! {"$range": [0, 1_000_000_000, 10_000]}, &doc! {}).unwrap();
    assert_eq!(got.as_array().unwrap().len(), 100_000);
    // Down, and empty.
    assert_eq!(
        ev(doc! {"$range": [3, 0, -1]}, &doc! {}).unwrap(),
        Bson::Array(vec![Bson::Int32(3), Bson::Int32(2), Bson::Int32(1)])
    );
    assert_eq!(ev(doc! {"$range": [3, 0]}, &doc! {}).unwrap(), Bson::Array(vec![]));
}

#[test]
fn range_counts_its_bytes_exactly_at_a_small_limit() {
    // Three Int32s: 5 bytes of framing, then 1 (type) + 1 (index) + 1 (terminator) + 4 for each.
    let args = [Bson::Int32(0), Bson::Int32(3)];
    let size = 5 + 3 * 7;
    assert_eq!(bson_bytes(&range_within(&args, size).unwrap()), size);
    let (bytes, max, text) = refusal(range_within(&args, size - 1));
    assert_eq!((bytes, max), (size, size - 1));
    assert!(text.starts_with("the array $range builds is "), "{text}");
    // An empty range is the framing alone.
    let none = [Bson::Int32(3), Bson::Int32(3)];
    assert_eq!(range_within(&none, 5).unwrap(), Bson::Array(vec![]));
    assert!(range_within(&none, 4).is_err());
    // The size is refused before the array is built: two billion elements under a
    // limit of a few bytes never allocate.
    let (bytes, _, _) = refusal(range_within(&[Bson::Int32(0), Bson::Int32(i32::MAX)], 64));
    assert_eq!(bytes, range_bytes(i32::MAX as u64));
    assert!(bytes > 20_000_000_000, "{bytes}");
    // The index grows a digit at 10, 100, ...: the closed form is the encoding.
    for count in [0u64, 1, 9, 10, 11, 99, 100, 101, 999, 1000, 1001, 12_345] {
        let args = [Bson::Int64(0), Bson::Int64(count as i64)];
        let built = range_within(&args, usize::MAX).unwrap();
        assert_eq!(range_bytes(count), bson_bytes(&built), "{count}");
        assert_eq!(bson_bytes(&built), encoded(&built), "{count}");
    }
    // A count no array can hold saturates instead of overflowing.
    assert!(range_bytes(u64::MAX) > 1 << 60);
}

fn text_of(bytes: usize) -> Bson {
    Bson::String("x".repeat(bytes))
}

#[test]
fn a_document_budget_is_exact_at_its_limit() {
    // {"a": "xxxx", "b": "yy"}: 5, then 2 + 1 + 9 and 2 + 1 + 7.
    let fields =
        [("a".to_string(), Expr::Literal(s("xxxx"))), ("b".to_string(), Expr::Literal(s("yy")))];
    let scope_doc = doc! {};
    let scope = Scope::new(&scope_doc);
    let built = eval_object(&fields, &scope, 27).unwrap();
    assert_eq!(built, Bson::Document(doc! {"a": "xxxx", "b": "yy"}));
    assert_eq!(bson_bytes(&built), 27);
    let (bytes, max, text) = refusal(eval_object(&fields, &scope, 26));
    assert_eq!((bytes, max), (27, 26));
    assert_eq!(
        text,
        "the document an object literal builds is 27 bytes, over the 16 MiB document limit; \
         build a smaller document, or leave out the fields that make it large"
    );
    // It stops at the field that passes: the first alone fits in 17 bytes.
    assert!(eval_object(&fields[..1], &scope, 17).is_ok());
    let (bytes, _, _) = refusal(eval_object(&fields, &scope, 17));
    assert_eq!(bytes, 27);
    // A field the expression leaves out (`$$REMOVE`) is not counted.
    let gone = [
        ("a".to_string(), Expr::Literal(s("xxxx"))),
        ("b".to_string(), Expr::parse(&Bson::String("$$REMOVE".into())).unwrap()),
    ];
    assert!(eval_object(&gone, &scope, 17).is_ok());
}

#[test]
fn doubling_an_object_is_refused_at_the_step_that_passes_the_limit() {
    // {l: V, r: V} each step, with a limit of 4 KiB: each step a little more than
    // doubles the last, and the step that would pass 4096 is the one refused.
    let limit = 4096;
    let scope_doc = doc! {};
    let scope = Scope::new(&scope_doc);
    let mut value = Bson::Document(doc! {"a": "x"});
    let mut steps = 0;
    let refused_at = loop {
        let fields = [
            ("l".to_string(), Expr::Literal(value.clone())),
            ("r".to_string(), Expr::Literal(value.clone())),
        ];
        match eval_object(&fields, &scope, limit) {
            Ok(next) => {
                assert!(bson_bytes(&next) <= limit);
                value = next;
                steps += 1;
            }
            Err(Error::ValueTooLarge { bytes, .. }) => break bytes,
            Err(other) => panic!("{other:?}"),
        }
    };
    assert!((5..12).contains(&steps), "{steps}");
    // The step that was refused would have been over the limit, and the one before
    // it was within.
    assert!(refused_at > limit && bson_bytes(&value) <= limit);
    assert!(bson_bytes(&value) * 2 > limit - 32, "{} {limit}", bson_bytes(&value));
}

/// `$reduce` over `$range: [0, steps]`, starting from `{a: "x"}`, with `body` as
/// `in`. The body copies `$$value` twice a step, so the document doubles.
fn doubling(body: Document, steps: i32) -> Result<Bson> {
    ev(
        doc! {"$reduce": {
            "input": {"$range": [0, steps]},
            "initialValue": {"a": "x"},
            "in": body,
        }},
        &doc! {},
    )
}

/// Twelve steps is a few kilobytes and answers. Thirty would be a billion times
/// that: refused as the document passes 16 MiB, holding a few copies of it and
/// not gigabytes (the reference shares the copies; this server cannot).
fn doubling_is_refused_at_16_mib(body: Document) {
    assert!(matches!(doubling(body.clone(), 12).unwrap(), Bson::Document(_)));
    let (bytes, max, text) = refusal(doubling(body, 30));
    assert_eq!(max, MAX_ARRAY_BYTES);
    assert!(bytes > MAX_ARRAY_BYTES && bytes < 2 * MAX_ARRAY_BYTES + 1024, "{bytes}");
    assert!(text.contains("builds is"), "{text}");
    assert!(text.contains("16 MiB document limit"), "{text}");
}

#[test]
fn a_reduce_that_doubles_an_object_literal_stops_at_16_mib() {
    doubling_is_refused_at_16_mib(doc! {"l": "$$value", "r": "$$value"});
}

#[test]
fn a_reduce_that_doubles_with_merge_objects_stops_at_16_mib() {
    doubling_is_refused_at_16_mib(doc! {"$mergeObjects": [{"l": "$$value"}, {"r": "$$value"}]});
}

#[test]
fn a_reduce_that_doubles_with_set_field_stops_at_16_mib() {
    doubling_is_refused_at_16_mib(
        doc! {"$setField": {"field": "r", "input": {"l": "$$value"}, "value": "$$value"}},
    );
}

#[test]
fn merge_objects_counts_a_replaced_key_once() {
    // Each argument sets `a` to 100 bytes. A thousand of them leave a document of
    // one field, so a limit that fits one fits all.
    let one = doc! {"a": "x".repeat(100)};
    let size = bson_bytes(&Bson::Document(one.clone()));
    let many = vec![Bson::Document(one.clone()); 1000];
    assert_eq!(merge_objects_within(&many, size).unwrap(), Bson::Document(one.clone()));
    let (bytes, max, text) = refusal(merge_objects_within(&many, size - 1));
    assert_eq!((bytes, max), (size, size - 1));
    assert!(text.starts_with("the document $mergeObjects builds is "), "{text}");
    // A shorter value for the same key makes room: the old one is taken off.
    let args = [Bson::Document(one), Bson::Document(doc! {"a": "y"}), Bson::Null];
    assert_eq!(merge_objects_within(&args, size).unwrap(), Bson::Document(doc! {"a": "y"}));
    // Distinct keys add up, and the refusal comes at the field that passes.
    let args: Vec<Bson> = (0..50).map(|i| Bson::Document(doc! {format!("k{i:02}"): 1})).collect();
    let all = bson_bytes(&merge_objects_within(&args, usize::MAX).unwrap());
    assert!(merge_objects_within(&args, all).is_ok());
    let (bytes, _, _) = refusal(merge_objects_within(&args, all - 1));
    assert_eq!(bytes, all);
    // A value that is not a document is the type error, in argument order.
    let err = merge_objects_within(&[Bson::Int32(1)], 5).unwrap_err();
    assert!(matches!(err, Error::InvalidQuery(_)), "{err:?}");
}

#[test]
fn set_field_counts_the_document_it_leaves() {
    let input = doc! {"a": "x".repeat(50), "b": 1};
    let size = bson_bytes(&Bson::Document(input.clone()));
    // Replacing a field with one of the same size leaves the size as it was.
    let mut doc = input.clone();
    set_field_within(&mut doc, "a", Some(text_of(50)), size).unwrap();
    assert_eq!(doc, input);
    // A new field is counted on top.
    let extra = 2 + 1 + 4;
    let mut doc = input.clone();
    set_field_within(&mut doc, "c", Some(Bson::Int32(1)), size + extra).unwrap();
    assert_eq!(doc.keys().collect::<Vec<_>>(), ["a", "b", "c"]);
    let mut doc = input.clone();
    let err = set_field_within(&mut doc, "c", Some(Bson::Int32(1)), size + extra - 1);
    let (bytes, max, text) = refusal(err.map(|()| Bson::Null));
    assert_eq!((bytes, max), (size + extra, size + extra - 1));
    assert!(text.starts_with("the document $setField builds is "), "{text}");
    // The refusal came before the store.
    assert_eq!(doc, input);
    // A value that grows an existing field is counted for the difference.
    let mut doc = input.clone();
    set_field_within(&mut doc, "a", Some(text_of(60)), size + 10).unwrap();
    let mut doc = input.clone();
    assert!(set_field_within(&mut doc, "a", Some(text_of(61)), size + 10).is_err());
    // A missing value removes the field, whatever the limit.
    let mut doc = input.clone();
    set_field_within(&mut doc, "a", None, 5).unwrap();
    assert_eq!(doc, doc! {"b": 1});
}

#[test]
fn a_set_field_through_the_operator_is_bounded() {
    let d = doc! {"big": "x".repeat(9 << 20)};
    // Nine megabytes set into a document that holds nine already: 18 MiB.
    let (bytes, max, text) = refusal(ev(
        doc! {"$setField": {"field": "b", "input": {"a": "$big"}, "value": "$big"}},
        &d,
    ));
    assert_eq!(max, MAX_ARRAY_BYTES);
    assert!(bytes > MAX_ARRAY_BYTES, "{bytes}");
    assert!(
        text.starts_with("the document an object literal builds is ")
            || text.starts_with("the document $setField builds is "),
        "{text}"
    );
    // One field replaced by one of its own size is within the limit.
    let got = ev(doc! {"$setField": {"field": "a", "input": {"a": "$big"}, "value": "$big"}}, &d);
    assert!(got.is_ok(), "{got:?}");
}

#[test]
fn array_to_object_counts_each_pair_before_it_stores_it() {
    let pairs =
        Bson::Array(vec![Bson::Array(vec![s("a"), s("xxxx")]), Bson::Array(vec![s("b"), s("yy")])]);
    // The same 27 bytes as the object literal of that name.
    assert_eq!(
        array_to_object_within(&pairs, 27).unwrap(),
        Bson::Document(doc! {"a": "xxxx", "b": "yy"})
    );
    let (bytes, max, text) = refusal(array_to_object_within(&pairs, 26));
    assert_eq!((bytes, max), (27, 26));
    assert!(text.starts_with("the document $arrayToObject builds is "), "{text}");
    // A key that comes again is counted once, at the size of its last value: the
    // first value alone takes 2 + 1 + 505 bytes, and the second replaces it.
    let again = Bson::Array(vec![
        Bson::Array(vec![s("a"), text_of(500)]),
        Bson::Array(vec![s("a"), s("xxxx")]),
    ]);
    let first = 5 + 2 + 1 + 505;
    assert_eq!(array_to_object_within(&again, first).unwrap(), Bson::Document(doc! {"a": "xxxx"}));
    assert!(array_to_object_within(&again, first - 1).is_err());
    // The {k, v} form is counted the same way.
    let docs = Bson::Array(vec![
        Bson::Document(doc! {"k": "a", "v": "xxxx"}),
        Bson::Document(doc! {"k": "b", "v": "yy"}),
    ]);
    assert!(array_to_object_within(&docs, 27).is_ok());
    assert!(array_to_object_within(&docs, 26).is_err());
    // Null is null, and the type errors come first, whatever the limit.
    assert_eq!(array_to_object_within(&Bson::Null, 0).unwrap(), Bson::Null);
    assert!(matches!(
        array_to_object_within(&Bson::Array(vec![Bson::Int32(1)]), 0),
        Err(Error::InvalidQuery(_))
    ));
}

#[test]
fn the_merge_objects_accumulator_counts_what_it_has_merged() {
    // One budget for the group: each document's fields are counted as they merge.
    let mut into = Document::new();
    let mut budget = merge_budget(5 + 3 * 8);
    for i in 0..3 {
        merge_into(&mut into, &mut budget, &Bson::Document(doc! {format!("k{i}"): 1})).unwrap();
    }
    // The same keys again replace and cost nothing.
    merge_into(&mut into, &mut budget, &Bson::Document(doc! {"k0": 2})).unwrap();
    assert_eq!(into.len(), 3);
    let err = merge_into(&mut into, &mut budget, &Bson::Document(doc! {"k3": 1})).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
    // The field that was refused was not stored, and null is skipped.
    assert_eq!(into.len(), 3);
    merge_into(&mut into, &mut budget, &Bson::Null).unwrap();
}

#[test]
fn the_n_forms_of_the_array_accumulators_stay_within_the_limit() {
    let d = doc! {"a": [3, 1, 2]};
    let got = ev(doc! {"$maxN": {"n": 2, "input": "$a"}}, &d).unwrap();
    assert_eq!(got, Bson::Array(vec![Bson::Int32(3), Bson::Int32(2)]));
}
