//! A4: a date shifted by NaN, an infinity or past the range is refused (q7s4b:220 to 260, 391 to
//! 393, 788 to 793). Ours answered `Date(0)` for a NaN and saturated for the rest.

use super::common::run;
use bson::{Bson, DateTime, doc};

fn eval(op: &str, args: [Bson; 2]) -> Result<Bson, String> {
    let pipeline = [doc! { "$project": { "_id": 0, "r": { op: args.to_vec() } } }];
    let out = run(&pipeline, vec![doc! { "_id": 1 }])?;
    Ok(out[0].get("r").cloned().expect("r"))
}

fn date(millis: i64) -> Bson {
    Bson::DateTime(DateTime::from_millis(millis))
}

#[test]
fn add_refuses_a_nan_and_an_infinity_in_either_position() {
    // b:220 NaN + Date, b:240 Inf + Date, b:260 -Inf + Date, b:391 to b:393 with the date first.
    for shift in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = eval("$add", [Bson::Double(shift), date(0)]).unwrap_err();
        assert!(err.contains("date overflow in $add"), "{shift}: {err}");
        eval("$add", [date(0), Bson::Double(shift)]).unwrap_err();
    }
}

#[test]
fn subtract_refuses_a_nan_an_infinity_and_long_min() {
    // b:791 to b:793 (31109), b:788 (159).
    for (shift, word) in [(f64::NAN, "nan"), (f64::INFINITY, "inf"), (f64::NEG_INFINITY, "-inf")] {
        let err = eval("$subtract", [date(0), Bson::Double(shift)]).unwrap_err();
        assert!(err.contains(&format!("out of range value {word} to long")), "{err}");
    }
    let err = eval("$subtract", [date(0), Bson::Int64(i64::MIN)]).unwrap_err();
    assert!(err.contains("Cannot negate the minimum duration"), "{err}");
}

#[test]
fn a_finite_double_past_the_long_range_and_an_overflowing_sum_are_refused() {
    // UNRECORDED: refused as the NaN is, where ours saturated.
    eval("$add", [date(0), Bson::Double(1e300)]).unwrap_err();
    eval("$add", [date(1), Bson::Int64(i64::MAX)]).unwrap_err();
    eval("$add", [date(-2), Bson::Int64(i64::MIN)]).unwrap_err();
    eval("$subtract", [date(0), Bson::Double(-1e19)]).unwrap_err();
    eval("$subtract", [date(-2), Bson::Int64(i64::MAX)]).unwrap_err();
    eval("$subtract", [date(2), Bson::Int64(-i64::MAX)]).unwrap_err();
}

#[test]
fn a_shift_in_range_answers_as_before() {
    // b:389 and b:789: a double is cut toward zero. b:390, b:790: -0.0. b:140, b:160, b:387,
    // b:787: a date no client prints is still a date (values from the sums, not recorded).
    assert_eq!(eval("$add", [date(0), Bson::Double(1.5)]), Ok(date(1)));
    assert_eq!(eval("$add", [date(0), Bson::Double(-1.5)]), Ok(date(-1)));
    assert_eq!(eval("$subtract", [date(0), Bson::Double(1.5)]), Ok(date(-1)));
    assert_eq!(eval("$add", [date(0), Bson::Double(-0.0)]), Ok(date(0)));
    assert_eq!(eval("$subtract", [date(0), Bson::Double(-0.0)]), Ok(date(0)));
    assert_eq!(eval("$add", [Bson::Int64(i64::MAX), date(0)]), Ok(date(i64::MAX)));
    assert_eq!(eval("$add", [date(0), Bson::Int64(i64::MIN)]), Ok(date(i64::MIN)));
    assert_eq!(eval("$subtract", [date(0), Bson::Int64(i64::MAX)]), Ok(date(-i64::MAX)));
    // A long past 2^53 is added exactly, not through a double.
    assert_eq!(eval("$add", [date(1), Bson::Int64((1 << 53) + 1)]), Ok(date((1 << 53) + 2)));
    assert_eq!(eval("$subtract", [date(0), Bson::Int64((1 << 53) + 1)]), Ok(date(-(1 << 53) - 1)));
    // Null stays null, a date minus a date is the interval.
    assert_eq!(eval("$add", [date(0), Bson::Null]), Ok(Bson::Null));
    assert_eq!(eval("$subtract", [date(5), date(2)]), Ok(Bson::Int64(3)));
}

#[test]
fn a_pipeline_update_does_not_store_a_date_from_a_nan() {
    use kimmy_query::update_pipeline::UpdatePipeline;
    let pipeline = UpdatePipeline::parse(&[Bson::Document(
        doc! { "$set": { "at": { "$add": ["$at", "$shift"] } } },
    )])
    .unwrap();
    let mut stored = doc! { "_id": 1, "at": date(0), "shift": f64::NAN };
    assert!(pipeline.apply(&mut stored).is_err());
}
