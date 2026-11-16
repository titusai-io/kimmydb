//! A12: `$divide` and `$mod` answer null for a null or missing operand in either position, before
//! the other operand's type is checked (q7s4b:1556, 1557, 1576, 1577, 1596, 1597 and 1956, 1957,
//! 1976, 1977, 1996, 1997). Ours refused the first operand that was not a number.

use super::common::run;
use bson::{Bson, DateTime, doc};

fn eval(op: &str, args: [Bson; 2]) -> Result<Bson, String> {
    let pipeline = [doc! { "$project": { "_id": 0, "r": { op: args.to_vec() } } }];
    let out = run(&pipeline, vec![doc! { "_id": 1 }])?;
    Ok(out[0].get("r").cloned().expect("r"))
}

#[test]
fn a_null_or_missing_operand_answers_null_before_a_type_is_checked() {
    let bad = [
        Bson::String("s".into()),
        Bson::Boolean(true),
        Bson::DateTime(DateTime::from_millis(0)),
        Bson::Array(vec![]),
    ];
    for op in ["$divide", "$mod"] {
        for first in &bad {
            // b:1556, b:1576, b:1596 (null) and b:1557 (a missing path).
            assert_eq!(eval(op, [first.clone(), Bson::Null]), Ok(Bson::Null), "{op} {first}");
            assert_eq!(eval(op, [first.clone(), "$nope".into()]), Ok(Bson::Null), "{op} {first}");
            // The null first, which answered null already.
            assert_eq!(eval(op, [Bson::Null, first.clone()]), Ok(Bson::Null), "{op} {first}");
        }
        assert_eq!(eval(op, [Bson::Null, Bson::Null]), Ok(Bson::Null));
    }
}

#[test]
fn two_operands_that_are_no_numbers_and_no_nulls_are_still_refused() {
    // b:1558 `$divide ["s", "s"]`; b:1555 `["s", DecNaN]`.
    for op in ["$divide", "$mod"] {
        eval(op, ["s".into(), "s".into()]).unwrap_err();
        eval(op, ["s".into(), 1.into()]).unwrap_err();
        eval(op, [1.into(), "s".into()]).unwrap_err();
    }
    assert_eq!(eval("$divide", [6.into(), 3.into()]), Ok(Bson::Double(2.0)));
    assert_eq!(eval("$mod", [7.into(), 3.into()]), Ok(Bson::Int32(1)));
}
