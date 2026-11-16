//! A14: `$ifNull` takes any number of arguments from two up: `[e1, ..., en, replacement]`
//! (q7s4b:3657, 3658, 3661). One argument and none are refused (3659, 3660).

use super::common::{refused, run};
use bson::{Bson, doc};

fn eval(args: Vec<Bson>) -> Result<Bson, String> {
    let pipeline = [doc! { "$project": { "_id": 0, "r": { "$ifNull": args } } }];
    let out = run(&pipeline, vec![doc! { "_id": 1, "n": Bson::Null, "k": 7 }])?;
    Ok(out[0].get("r").cloned().unwrap_or(Bson::String("<missing>".into())))
}

#[test]
fn the_first_argument_that_is_neither_null_nor_missing_answers() {
    // b:3657, b:3658, b:3661
    assert_eq!(eval(vec![Bson::Null, Bson::Null, 3.into()]), Ok(Bson::Int32(3)));
    assert_eq!(eval(vec![Bson::Null, 2.into(), 3.into()]), Ok(Bson::Int32(2)));
    assert_eq!(eval(vec!["$nope".into(), "$nope".into(), "$$ROOT.k".into()]), Ok(Bson::Int32(7)));
    assert_eq!(eval(vec!["$nope".into(), "$n".into(), "$k".into(), 9.into()]), Ok(Bson::Int32(7)));
    // Four and five arguments, the answer in each position.
    assert_eq!(eval(vec![1.into(), 2.into(), 3.into(), 4.into()]), Ok(Bson::Int32(1)));
    assert_eq!(eval(vec![Bson::Null, Bson::Null, Bson::Null, 4.into()]), Ok(Bson::Int32(4)));
}

#[test]
fn the_replacement_is_the_answer_when_every_other_argument_is_null() {
    // b:3631 `[null, null]` is null, b:3634 `[missing, null]` is null.
    assert_eq!(eval(vec![Bson::Null, Bson::Null, Bson::Null]), Ok(Bson::Null));
    assert_eq!(eval(vec!["$nope".into(), Bson::Null]), Ok(Bson::Null));
    // The two-argument form is unchanged (b:3630, b:3633, b:3636).
    assert_eq!(eval(vec![Bson::Null, 9.into()]), Ok(Bson::Int32(9)));
    assert_eq!(eval(vec!["s".into(), 9.into()]), Ok("s".into()));
}

#[test]
fn the_arguments_after_the_answer_are_not_read() {
    // Lazy, as `$cond` is: a later argument that would fail is not read. UNRECORDED in the
    // reference for an erroring replacement that is not taken.
    let failing = bson::bson!({ "$divide": [1, "s"] });
    assert_eq!(eval(vec![1.into(), failing.clone()]), Ok(Bson::Int32(1)));
    assert_eq!(eval(vec![Bson::Null, 2.into(), failing.clone()]), Ok(Bson::Int32(2)));
    eval(vec![Bson::Null, Bson::Null, failing]).unwrap_err();
}

#[test]
fn one_argument_and_none_are_refused() {
    // b:3659, b:3660
    for args in [vec![Bson::Int32(1)], vec![]] {
        let pipeline = [doc! { "$project": { "r": { "$ifNull": args } } }];
        assert!(refused(&pipeline).contains("$ifNull"), "{pipeline:?}");
    }
}
