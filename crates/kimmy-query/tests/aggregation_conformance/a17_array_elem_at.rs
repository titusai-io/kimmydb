//! A17: `$arrayElemAt` with an index that is not a 32-bit integer is refused (q7s4c:136, an
//! Int64 2147483648: code 28691), where ours answered null.

use super::common::run;
use bson::{Bson, doc};

fn at(index: Bson) -> Result<Bson, String> {
    let pipeline = [doc! { "$project": { "_id": 0, "r": { "$arrayElemAt": [[1, 2, 3], index] } } }];
    let out = run(&pipeline, vec![doc! { "_id": 1 }])?;
    Ok(out[0].get("r").cloned().unwrap_or(Bson::Null))
}

#[test]
fn an_index_past_int32_is_refused() {
    // c:136
    let err = at(Bson::Int64(2_147_483_648)).unwrap_err();
    assert!(err.contains("32-bit integer"), "{err}");
    at(Bson::Int64(-2_147_483_649)).unwrap_err();
    at(Bson::Int64(i64::MAX)).unwrap_err();
    at(Bson::Double(3e9)).unwrap_err();
}

#[test]
fn an_index_within_int32_answers_as_before() {
    // c:125 (1.0), c:133 (Int64), c:134 (Decimal), c:128 (null); out of range stays null.
    assert_eq!(at(Bson::Double(1.0)), Ok(Bson::Int32(2)));
    assert_eq!(at(Bson::Int64(1)), Ok(Bson::Int32(2)));
    assert_eq!(at(Bson::Null), Ok(Bson::Null));
    assert_eq!(at(Bson::Int64(i64::from(i32::MAX))), Ok(Bson::Null));
    assert_eq!(at(Bson::Int64(i64::from(i32::MIN))), Ok(Bson::Null));
    assert_eq!(at(Bson::Int32(-1)), Ok(Bson::Int32(3)));
}
