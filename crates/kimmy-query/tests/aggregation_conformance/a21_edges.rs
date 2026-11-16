//! The small differences of q7s4g 670 to 681: the sign of a sum of negative zeros, the unit
//! `$substr` counts in, and the index `$arrayElemAt` accepts.

use super::common::run;
use bson::{Bson, doc};

fn eval(expr: Bson) -> Result<Bson, String> {
    let out = run(&[doc! { "$project": { "_id": 0, "r": expr } }], vec![doc! { "_id": 1 }])?;
    Ok(out[0].get("r").cloned().unwrap())
}

fn positive_zero(value: Bson) -> bool {
    matches!(value, Bson::Double(d) if d == 0.0 && d.is_sign_positive())
}

fn negative_zero(value: Bson) -> bool {
    matches!(value, Bson::Double(d) if d == 0.0 && d.is_sign_negative())
}

#[test]
fn a_sum_of_negative_zeros_is_positive_zero_and_a_product_keeps_its_sign() {
    // g:670 `$add [-0.0, -0.0]` and g:671 `$add [-0.0]` are 0.0; g:672 `$multiply [-0.0, 1]` is
    // -0.0.
    assert!(positive_zero(eval(bson::bson!({ "$add": [-0.0, -0.0] })).unwrap()));
    assert!(positive_zero(eval(bson::bson!({ "$add": [-0.0] })).unwrap()));
    assert!(positive_zero(eval(bson::bson!({ "$add": [-0.0, 0.0, -0.0] })).unwrap()));
    assert!(negative_zero(eval(bson::bson!({ "$multiply": [-0.0, 1] })).unwrap()));
    // A sum that is not zero is as it was.
    assert_eq!(eval(bson::bson!({ "$add": [-0.0, 1.5] })), Ok(Bson::Double(1.5)));
}

#[test]
fn the_legacy_substr_counts_bytes_and_substr_cp_code_points() {
    // g:674 `$substr ["aéb", 1, 2]` is "é" (bytes), g:675 `$substrBytes` the same, g:676
    // `$substrCP` is "éb"; g:678 a negative length of `$substr` is to the end, g:677 is refused
    // for `$substrCP`.
    assert_eq!(eval(bson::bson!({ "$substr": ["aéb", 1, 2] })), Ok("é".into()));
    assert_eq!(eval(bson::bson!({ "$substrBytes": ["aéb", 1, 2] })), Ok("é".into()));
    assert_eq!(eval(bson::bson!({ "$substrCP": ["aéb", 1, 2] })), Ok("éb".into()));
    assert_eq!(eval(bson::bson!({ "$substr": ["abc", 1, -1] })), Ok("bc".into()));
    eval(bson::bson!({ "$substrCP": ["abc", 1, -1] })).unwrap_err();
    // A range that splits a character is refused, as `$substrBytes` refuses it (q8str:130).
    eval(bson::bson!({ "$substr": ["aéb", 2, 1] })).unwrap_err();
}

#[test]
fn an_array_index_past_int32_is_refused_whatever_type_it_is_written_in() {
    // g:679 Int64 2147483648, g:681 the double 2^31: 28691; g:680 Int64 1 answers.
    eval(bson::bson!({ "$arrayElemAt": [[1, 2], 2_147_483_648_i64] })).unwrap_err();
    eval(bson::bson!({ "$arrayElemAt": [[1, 2], 2_147_483_648.0] })).unwrap_err();
    assert_eq!(eval(bson::bson!({ "$arrayElemAt": [[1, 2], 1_i64] })), Ok(Bson::Int32(2)));
}
