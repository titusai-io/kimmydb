//! The size limits of the expression layer: a string or an array an expression
//! builds is at most 16 MiB, refused before it is built, with a `413` that names
//! what hit which limit. The limits are parameters of the `*_within` functions, so
//! a test reaches them with a few bytes; the operators that read the real limit
//! are tested with values a megabyte wide.

use bson::{Bson, Document, doc};
use kimmy_core::{Error, Result};

use super::sizes::{ArrayBudget, bson_bytes, check_array_within};
use super::{Expr, MAX_ARRAY_BYTES, concat_arrays_within, object_to_array_within, split_within};

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
    // A span of two billion is refused by its count, not built (a 400, as it was).
    for args in [
        vec![Bson::Int32(0), Bson::Int32(i32::MAX)],
        vec![Bson::Int64(0), Bson::Int64(i64::from(i32::MAX))],
        vec![Bson::Int64(i64::from(i32::MIN)), Bson::Int64(i64::from(i32::MAX)), Bson::Int32(1)],
        vec![Bson::Int32(0), Bson::Int32(i32::MAX), Bson::Int32(2)],
    ] {
        let err = ev(doc! {"$range": args}, &doc! {}).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidQuery(m) if m.contains("over the limit of 100000")),
            "{err:?}"
        );
    }
    // A bound past `Int32` is refused as a 32-bit bound, as the reference refuses it (34446).
    let err = ev(doc! {"$range": [Bson::Int64(0), Bson::Int64(i64::MAX)]}, &doc! {}).unwrap_err();
    assert!(matches!(&err, Error::InvalidQuery(m) if m.contains("32-bit integer")), "{err:?}");
    assert_eq!(
        ev(doc! {"$range": [0, 100_000]}, &doc! {}).unwrap().as_array().unwrap().len(),
        100_000
    );
}

#[test]
fn the_n_forms_of_the_array_accumulators_stay_within_the_limit() {
    let d = doc! {"a": [3, 1, 2]};
    let got = ev(doc! {"$maxN": {"n": 2, "input": "$a"}}, &d).unwrap();
    assert_eq!(got, Bson::Array(vec![Bson::Int32(3), Bson::Int32(2)]));
}
