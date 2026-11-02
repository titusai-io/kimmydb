//! `$binarySize`, `$bsonSize`, `$tsSecond` and `$tsIncrement`: the reference's answers
//! (recorded from the reference (6.0.5) by `q8-probes/q8d-size.js`) for every type of operand, the
//! type of the answer, and the words each refusal is in.

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, Timestamp, doc};

use super::Expr;

fn eval(expr: Document, d: &Document) -> kimmy_core::Result<Bson> {
    Expr::parse(&Bson::Document(expr))?.eval(d)
}

fn val(expr: Document) -> Bson {
    eval(expr, &Document::new()).unwrap_or_else(|e| panic!("{e}"))
}

/// The words of a refusal, without the class the error prints in front of them.
fn refused(expr: Document) -> String {
    let text = eval(expr, &Document::new()).expect_err("must be refused").to_string();
    text.strip_prefix("invalid query: ").unwrap_or(&text).to_string()
}

fn bin(bytes: &[u8]) -> Bson {
    Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: bytes.to_vec() })
}

fn s(text: &str) -> Bson {
    Bson::String(text.into())
}

#[test]
fn binary_size_counts_the_bytes_of_a_string_or_of_binary_data() {
    let size = |v: Bson| val(doc! { "$binarySize": v });
    assert_eq!(size(s("")), Bson::Int32(0));
    assert_eq!(size(s("abc")), Bson::Int32(3));
    // UTF-8 bytes: two, three and four for one character each.
    assert_eq!(size(s("é")), Bson::Int32(2));
    assert_eq!(size(s("€")), Bson::Int32(3));
    assert_eq!(size(s("😀")), Bson::Int32(4));
    assert_eq!(size(s("aé€😀")), Bson::Int32(10));
    assert_eq!(size(s(&"x".repeat(1000))), Bson::Int32(1000));
    assert_eq!(size(s("a\0b")), Bson::Int32(3));
    assert_eq!(size(bin(&[])), Bson::Int32(0));
    assert_eq!(size(bin(&[1, 2, 3])), Bson::Int32(3));
    assert_eq!(size(bin(&(0..16).collect::<Vec<u8>>())), Bson::Int32(16));
    assert_eq!(size(Bson::Null), Bson::Null);
    assert_eq!(size(s("$missing")), Bson::Null);
}

#[test]
fn binary_size_reads_a_field_and_a_one_element_list() {
    let d = doc! { "s": "str", "b": bin(&[1, 2, 3]), "arr": [1, 2, 3] };
    let size = |v: Bson| eval(doc! { "$binarySize": v }, &d);
    assert_eq!(size(s("$s")).unwrap(), Bson::Int32(3));
    assert_eq!(size(s("$b")).unwrap(), Bson::Int32(3));
    assert_eq!(size(Bson::Array(vec![s("abc")])).unwrap(), Bson::Int32(3));
    assert_eq!(size(Bson::Array(vec![Bson::Null])).unwrap(), Bson::Null);
    assert_eq!(
        size(s("$arr")).unwrap_err().to_string(),
        "invalid query: $binarySize requires a string or BinData argument, found: array"
    );
}

#[test]
fn binary_size_refuses_every_other_type() {
    for (operand, name) in [
        (Bson::Int32(5), "int"),
        (Bson::Int64(5), "long"),
        (Bson::Double(1.5), "double"),
        (Bson::Decimal128("1".parse().unwrap()), "decimal"),
        (Bson::Boolean(true), "bool"),
        (Bson::DateTime(bson::DateTime::from_millis(0)), "date"),
        (Bson::Document(doc! { "a": 1 }), "object"),
        (Bson::ObjectId(bson::oid::ObjectId::from_bytes([0; 12])), "objectId"),
        (Bson::Timestamp(Timestamp { time: 1, increment: 2 }), "timestamp"),
        (Bson::MinKey, "minKey"),
        (Bson::MaxKey, "maxKey"),
        (
            Bson::RegularExpression(bson::Regex {
                pattern: "a".to_string().try_into().expect("valid pattern"),
                options: String::new().try_into().expect("valid options"),
            }),
            "regex",
        ),
    ] {
        assert_eq!(
            refused(doc! { "$binarySize": [operand] }),
            format!("$binarySize requires a string or BinData argument, found: {name}")
        );
    }
}

#[test]
fn bson_size_is_the_encoded_size_of_a_document() {
    let size = |v: Document| val(doc! { "$bsonSize": { "$literal": v } });
    assert_eq!(size(doc! {}), Bson::Int32(5));
    assert_eq!(size(doc! { "a": 1 }), Bson::Int32(12));
    assert_eq!(size(doc! { "a": { "b": [1, 2, 3] } }), Bson::Int32(42));
    assert_eq!(size(doc! { "a": 1i64 }), Bson::Int32(16));
    assert_eq!(size(doc! { "a": 1.5 }), Bson::Int32(16));
    assert_eq!(size(doc! { "s": "héllo" }), Bson::Int32(19));
    assert_eq!(size(doc! { "d": Bson::Decimal128("1".parse().unwrap()) }), Bson::Int32(24));
    assert_eq!(size(doc! { "b": bin(&[1, 2, 3]) }), Bson::Int32(16));
    assert_eq!(size(doc! { "a": Vec::<Bson>::new() }), Bson::Int32(13));
    assert_eq!(size(doc! { "a": Bson::Null }), Bson::Int32(8));
    assert_eq!(size(doc! { "a": bson::DateTime::from_millis(0) }), Bson::Int32(16));
    assert_eq!(
        size(doc! {
            "aaaaaaaaaa": "x",
            "b": true,
            "c": bson::oid::ObjectId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        }),
        Bson::Int32(42)
    );
}

#[test]
fn bson_size_of_a_document_that_is_read_or_built() {
    let d = doc! { "i": 7, "s": "str", "arr": [1, 2], "nul": Bson::Null };
    let size = |v: Bson| eval(doc! { "$bsonSize": v }, &d);
    let encoded = bson::serialize_to_vec(&d).unwrap().len();
    assert_eq!(size(s("$$ROOT")).unwrap(), Bson::Int32(encoded as i32));
    // A document built from fields: `{a: 7, b: "str"}` is 4 + (1+2+4) + (1+2+4+4) + 1.
    assert_eq!(size(Bson::Document(doc! { "a": "$i", "b": "$s" })).unwrap(), Bson::Int32(23));
    assert_eq!(size(s("$nul")).unwrap(), Bson::Null);
    assert_eq!(size(s("$nope")).unwrap(), Bson::Null);
    assert_eq!(
        size(Bson::Array(vec![Bson::Document(doc! { "$literal": { "a": 1 } })])).unwrap(),
        Bson::Int32(12)
    );
    assert_eq!(
        size(s("$arr")).unwrap_err().to_string(),
        "invalid query: $bsonSize requires a document input, found: array"
    );
}

#[test]
fn bson_size_refuses_a_value_that_is_not_a_document() {
    for (operand, name) in [
        (Bson::Int32(5), "int"),
        (s("abc"), "string"),
        (Bson::Boolean(true), "bool"),
        (Bson::Double(1.5), "double"),
        (Bson::Int64(1), "long"),
        (Bson::Decimal128("1".parse().unwrap()), "decimal"),
        (bin(&[1, 2, 3]), "binData"),
        (Bson::DateTime(bson::DateTime::from_millis(0)), "date"),
        (Bson::Timestamp(Timestamp { time: 1, increment: 2 }), "timestamp"),
    ] {
        // A bare string is a literal here, not a field: the operand is the value itself.
        assert_eq!(
            refused(doc! { "$bsonSize": [operand] }),
            format!("$bsonSize requires a document input, found: {name}")
        );
    }
}

#[test]
fn the_timestamp_parts_are_longs() {
    let t = Bson::Timestamp(Timestamp { time: 1_700_000_000, increment: 5 });
    assert_eq!(val(doc! { "$tsSecond": [t.clone()] }), Bson::Int64(1_700_000_000));
    assert_eq!(val(doc! { "$tsIncrement": [t.clone()] }), Bson::Int64(5));
    let zero = Bson::Timestamp(Timestamp { time: 0, increment: 0 });
    assert_eq!(val(doc! { "$tsSecond": [zero.clone()] }), Bson::Int64(0));
    assert_eq!(val(doc! { "$tsIncrement": [zero] }), Bson::Int64(0));
    // The full range of both halves.
    let top = Bson::Timestamp(Timestamp { time: u32::MAX, increment: u32::MAX });
    assert_eq!(val(doc! { "$tsSecond": [top.clone()] }), Bson::Int64(4_294_967_295));
    assert_eq!(val(doc! { "$tsIncrement": [top] }), Bson::Int64(4_294_967_295));
    let one = Bson::Timestamp(Timestamp { time: 1, increment: 0 });
    assert_eq!(val(doc! { "$tsSecond": [one.clone()] }), Bson::Int64(1));
    assert_eq!(val(doc! { "$tsIncrement": [one] }), Bson::Int64(0));
    // Read from a field, or as the only element of a list; null and missing are null.
    let d = doc! { "ts": t };
    assert_eq!(eval(doc! { "$tsSecond": "$ts" }, &d).unwrap(), Bson::Int64(1_700_000_000));
    assert_eq!(eval(doc! { "$tsIncrement": "$ts" }, &d).unwrap(), Bson::Int64(5));
    assert_eq!(eval(doc! { "$tsSecond": "$nope" }, &d).unwrap(), Bson::Null);
    assert_eq!(eval(doc! { "$tsIncrement": [Bson::Null] }, &d).unwrap(), Bson::Null);
}

#[test]
fn the_timestamp_parts_refuse_every_other_type() {
    for (operand, name) in [
        (Bson::Int32(5), "int"),
        (Bson::Int64(5), "long"),
        (Bson::Double(5.5), "double"),
        (Bson::Decimal128("5".parse().unwrap()), "decimal"),
        (s("a"), "string"),
        (Bson::DateTime(bson::DateTime::from_millis(1_700_000_000_000)), "date"),
        (Bson::Boolean(true), "bool"),
        (Bson::Document(doc! { "a": 1 }), "object"),
        (Bson::ObjectId(bson::oid::ObjectId::from_bytes([0; 12])), "objectId"),
        (bin(&[1, 2, 3]), "binData"),
    ] {
        for name_of_op in ["$tsSecond", "$tsIncrement"] {
            assert_eq!(
                refused(doc! { name_of_op: [operand.clone()] }),
                format!("Argument to {name_of_op} must be a timestamp, but is {name}")
            );
        }
    }
    let d = doc! { "arr": [1] };
    assert_eq!(
        eval(doc! { "$tsSecond": "$arr" }, &d).unwrap_err().to_string(),
        "invalid query: Argument to $tsSecond must be a timestamp, but is array"
    );
}

#[test]
fn each_takes_exactly_one_operand() {
    for name in ["$binarySize", "$bsonSize", "$tsSecond", "$tsIncrement"] {
        for args in [vec![], vec![Bson::Int32(1), Bson::Int32(2)]] {
            let found = args.len();
            let message = Expr::parse(&Bson::Document(doc! { name: args }))
                .expect_err("must be refused")
                .to_string();
            assert!(message.contains(&format!("found {found}")), "{name}: {message}");
        }
    }
}
