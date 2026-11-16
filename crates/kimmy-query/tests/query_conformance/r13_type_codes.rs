use super::common::{Case, assert_recorded, bin, dec, pick, re};
use bson::{Bson, Document, doc};

fn docs_q7s3f0228() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Double(1.5_f64) },
        doc! { "_id": Bson::Int32(2), "v": Bson::String("s".to_string()) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Array(vec![Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(5), "v": bin(&[1], 0x00) },
        doc! { "_id": Bson::Int32(6), "v": Bson::ObjectId(bson::oid::ObjectId::parse_str("000000000000000000000001").unwrap()) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Boolean(true) },
        doc! { "_id": Bson::Int32(8), "v": Bson::DateTime(bson::DateTime::from_millis(0)) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Null },
        doc! { "_id": Bson::Int32(10), "v": re("a", "") },
        doc! { "_id": Bson::Int32(11), "v": Bson::JavaScriptCode("x".to_string()) },
        doc! { "_id": Bson::Int32(12), "v": Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope { code: "x".to_string(), scope: doc! { "y": Bson::Int32(1) } }) },
        doc! { "_id": Bson::Int32(13), "v": Bson::Int32(5) },
        doc! { "_id": Bson::Int32(14), "v": Bson::Timestamp(bson::Timestamp { time: 1, increment: 1 }) },
        doc! { "_id": Bson::Int32(15), "v": Bson::Int64(5) },
        doc! { "_id": Bson::Int32(16), "v": dec("5") },
        doc! { "_id": Bson::Int32(17), "v": Bson::MinKey },
        doc! { "_id": Bson::Int32(18), "v": Bson::MaxKey },
        doc! { "_id": Bson::Int32(19), "v": Bson::Symbol("sym".to_string()) },
    ]
}
fn docs_q7s3b0097() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Int32(5) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Null },
        doc! { "_id": Bson::Int32(3) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3)])]) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
        doc! { "_id": Bson::Int32(9), "a": Bson::String("abc".to_string()) },
        doc! { "_id": Bson::Int32(10), "a": Bson::Array(vec![Bson::Null]) },
        doc! { "_id": Bson::Int32(11), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Null }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(12), "a": Bson::Double(7.5_f64) },
        doc! { "_id": Bson::Int32(13), "a": Bson::Array(vec![Bson::Int32(5), Bson::String("x".to_string()), Bson::Null]) },
        doc! { "_id": Bson::Int32(14), "a": Bson::Document(doc! { "b": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2) })]) }) },
        doc! { "_id": Bson::Int32(15), "a": Bson::Boolean(true) },
        doc! { "_id": Bson::Int32(16), "a": Bson::DateTime(bson::DateTime::from_millis(0)) },
        doc! { "_id": Bson::Int32(17), "a": Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Array(vec![Bson::Int32(3)])])]) },
        doc! { "_id": Bson::Int32(18), "a": Bson::String("ABC".to_string()) },
        doc! { "_id": Bson::Int32(19), "a": Bson::Int64(8) },
        doc! { "_id": Bson::Int32(20), "a": dec("9") },
        doc! { "_id": Bson::Int32(21), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
        doc! { "_id": Bson::Int32(22), "a": Bson::Document(Document::new()) },
        doc! { "_id": Bson::Int32(23), "a": Bson::Array(vec![Bson::Array(vec![])]) },
        doc! { "_id": Bson::Int32(24), "a": re("abc", "") },
    ]
}
pub fn cases_r13() -> Vec<Case> {
    vec![
        Case {
            label: "q7s3f0228",
            line: 3713,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(1) }) },
            ids: Some(vec![1]),
        },
        Case {
            label: "q7s3f0229",
            line: 3738,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(2) }) },
            ids: Some(vec![2]),
        },
        Case {
            label: "q7s3f0230",
            line: 3763,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(3) }) },
            ids: Some(vec![3]),
        },
        Case {
            label: "q7s3f0231",
            line: 3788,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(4) }) },
            ids: Some(vec![4]),
        },
        Case {
            label: "q7s3f0232",
            line: 3813,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(5) }) },
            ids: Some(vec![5]),
        },
        Case {
            label: "q7s3f0233",
            line: 3838,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(6) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3f0234",
            line: 3863,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(7) }) },
            ids: Some(vec![6]),
        },
        Case {
            label: "q7s3f0235",
            line: 3888,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(8) }) },
            ids: Some(vec![7]),
        },
        Case {
            label: "q7s3f0236",
            line: 3913,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(9) }) },
            ids: Some(vec![8]),
        },
        Case {
            label: "q7s3f0237",
            line: 3938,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(10) }) },
            ids: Some(vec![9]),
        },
        Case {
            label: "q7s3f0238",
            line: 3963,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(11) }) },
            ids: Some(vec![10]),
        },
        Case {
            label: "q7s3f0239",
            line: 3988,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(12) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3f0240",
            line: 4013,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(13) }) },
            ids: Some(vec![11]),
        },
        Case {
            label: "q7s3f0241",
            line: 4038,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(14) }) },
            ids: Some(vec![19]),
        },
        Case {
            label: "q7s3f0242",
            line: 4063,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(15) }) },
            ids: Some(vec![12]),
        },
        Case {
            label: "q7s3f0243",
            line: 4088,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(16) }) },
            ids: Some(vec![4, 13]),
        },
        Case {
            label: "q7s3f0244",
            line: 4113,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(17) }) },
            ids: Some(vec![14]),
        },
        Case {
            label: "q7s3f0245",
            line: 4138,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(18) }) },
            ids: Some(vec![15]),
        },
        Case {
            label: "q7s3f0246",
            line: 4163,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(19) }) },
            ids: Some(vec![16]),
        },
        Case {
            label: "q7s3f0247",
            line: 4188,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(20) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0248",
            line: 4213,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(127) }) },
            ids: Some(vec![18]),
        },
        Case {
            label: "q7s3f0249",
            line: 4238,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(255) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0250",
            line: 4263,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(-1) }) },
            ids: Some(vec![17]),
        },
        Case {
            label: "q7s3f0251",
            line: 4288,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(0) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0252",
            line: 4313,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int32(21) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0253",
            line: 4339,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(1.0_f64) }) },
            ids: Some(vec![1]),
        },
        Case {
            label: "q7s3f0254",
            line: 4364,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(2.0_f64) }) },
            ids: Some(vec![2]),
        },
        Case {
            label: "q7s3f0255",
            line: 4389,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(1.5_f64) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0256",
            line: 4414,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(12.0_f64) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3f0257",
            line: 4439,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(15.0_f64) }) },
            ids: Some(vec![12]),
        },
        Case {
            label: "q7s3f0258",
            line: 4464,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(-1.0_f64) }) },
            ids: Some(vec![17]),
        },
        Case {
            label: "q7s3f0259",
            line: 4489,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(f64::NAN) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0260",
            line: 4514,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Double(127.0_f64) }) },
            ids: Some(vec![18]),
        },
        Case {
            label: "q7s3f0261",
            line: 4539,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Int64(2) }) },
            ids: Some(vec![2]),
        },
        Case {
            label: "q7s3f0262",
            line: 4564,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": dec("2") }) },
            ids: Some(vec![2]),
        },
        Case {
            label: "q7s3f0263",
            line: 4589,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": dec("2.5") }) },
            ids: None,
        },
        Case {
            label: "q7s3f0264",
            line: 4614,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("javascript".to_string()) }) },
            ids: Some(vec![11]),
        },
        Case {
            label: "q7s3f0265",
            line: 4639,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("javascriptWithScope".to_string()) }) },
            ids: Some(vec![12]),
        },
        Case {
            label: "q7s3f0266",
            line: 4664,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("symbol".to_string()) }) },
            ids: Some(vec![19]),
        },
        Case {
            label: "q7s3f0267",
            line: 4689,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("dbPointer".to_string()) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3f0268",
            line: 4714,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("timestamp".to_string()) }) },
            ids: Some(vec![14]),
        },
        Case {
            label: "q7s3f0269",
            line: 4739,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("undefined".to_string()) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3f0270",
            line: 4764,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("minKey".to_string()) }) },
            ids: Some(vec![17]),
        },
        Case {
            label: "q7s3f0271",
            line: 4789,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("maxKey".to_string()) }) },
            ids: Some(vec![18]),
        },
        Case {
            label: "q7s3f0272",
            line: 4814,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            ids: Some(vec![1, 2]),
        },
        Case {
            label: "q7s3f0273",
            line: 4839,
            docs: docs_q7s3f0228,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::Array(vec![Bson::Int32(12), Bson::String("double".to_string())]) }) },
            ids: Some(vec![1]),
        },
        Case {
            label: "q7s3b0097",
            line: 2787,
            docs: docs_q7s3b0097,
            filter: doc! { "a": Bson::Document(doc! { "$type": Bson::Int32(14) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3b0123",
            line: 3541,
            docs: docs_q7s3b0097,
            filter: doc! { "a": Bson::Document(doc! { "$type": Bson::Double(2.0_f64) }) },
            ids: Some(vec![9, 13, 18]),
        },
    ]
}

/// Every `$type` over the 20 values of q7s3f0228 to f0273 (q7s3f.out:3713 to 4850) and
/// q7s3b0097 and b0123 (q7s3b.out:2787 and 3541): each code from -1 to 127, the refused ones
/// (0, 20, 21, 255, 1.5 and NaN) and the whole numbers written as a double, an `Int64` and a decimal.
#[test]
fn type_codes_answer_as_recorded() {
    assert_recorded("$type codes", cases_r13());
}

/// A whole number of any numeric type is a code: `Double(2)` is "string" (q7s3b0123, f0254), `Double(1)`
/// is "double" (f0253), `Double(-1)` is `minKey` and `Double(127)` is `maxKey` (f0258, f0260),
/// `Decimal(2)` is "string" (f0262); a fraction or a NaN is refused (f0255, f0259).
#[test]
fn a_whole_number_of_any_type_is_a_code() {
    let cases = pick(
        cases_r13(),
        &[
            "q7s3b0123",
            "q7s3f0253",
            "q7s3f0254",
            "q7s3f0255",
            "q7s3f0256",
            "q7s3f0257",
            "q7s3f0258",
            "q7s3f0259",
            "q7s3f0260",
            "q7s3f0261",
            "q7s3f0262",
        ],
    );
    assert_recorded("$type by a double, a long or a decimal", cases);
}

/// Codes 12, 14 and 15 and their aliases (q7s3b0097, q7s3f0239, f0241, f0242, f0264 to f0267,
/// f0273): `symbol` is held by a symbol, `dbPointer` by nothing a client can store, and code
/// with a scope (15, `javascriptWithScope`) by the value that has one and not by plain code
/// (13, `javascript`), whether written as a code or as an alias.
#[test]
fn the_legacy_codes_and_code_with_a_scope() {
    let cases = pick(
        cases_r13(),
        &[
            "q7s3b0097",
            "q7s3f0239",
            "q7s3f0240",
            "q7s3f0241",
            "q7s3f0242",
            "q7s3f0264",
            "q7s3f0265",
            "q7s3f0266",
            "q7s3f0267",
            "q7s3f0273",
        ],
    );
    assert_recorded("$type 12 to 15 and their aliases", cases);
}
