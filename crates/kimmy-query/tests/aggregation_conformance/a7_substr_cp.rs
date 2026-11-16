//! A7: `$substrCP` with a negative length is refused (q7s4d:137, code 34454, "length must be a
//! nonnegative integer"), where ours answered the rest of the string.

use super::common::run;
use bson::{Bson, doc};

fn substr(op: &str, args: [Bson; 3]) -> Result<Bson, String> {
    let out = run(
        &[doc! { "$project": { "_id": 0, "r": { op: args.to_vec() } } }],
        vec![doc! {
            "_id": 1
        }],
    )?;
    Ok(out[0].get("r").cloned().expect("r"))
}

#[test]
fn a_negative_length_is_refused_by_substr_cp() {
    // d:137
    for length in [Bson::Int32(-1), Bson::Int64(-1), Bson::Double(-1.0)] {
        let err = substr("$substrCP", ["abc".into(), 0.into(), length.clone()]).unwrap_err();
        assert!(err.contains("$substrCP length"), "{length}: {err}");
    }
}

#[test]
fn a_negative_start_is_still_refused_and_a_valid_range_is_unchanged() {
    // d:136 (34455) and q8extra:36: a negative start, refused already.
    substr("$substrCP", ["abc".into(), (-1).into(), 1.into()]).unwrap_err();
    assert_eq!(substr("$substrCP", ["héllo".into(), 1.into(), 2.into()]), Ok("él".into()));
    assert_eq!(substr("$substrCP", ["abc".into(), 0.into(), 0.into()]), Ok("".into()));
    assert_eq!(substr("$substrCP", ["abc".into(), 5.into(), 2.into()]), Ok("".into()));
}

#[test]
fn the_legacy_substr_keeps_a_negative_length_as_to_the_end() {
    // q8str:126 and :134, recorded for `$substrBytes`; `$substr` is the same legacy operator.
    assert_eq!(substr("$substr", ["abcdef".into(), 2.into(), (-1).into()]), Ok("cdef".into()));
}
