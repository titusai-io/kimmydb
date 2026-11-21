//! The names a stream and the metrics give a loss record ([`LossKind::as_str`], [`LossValue::is_cut`]).

use super::tests::p;
use super::*;

#[test]
fn the_kinds_have_chosen_names_and_an_index_that_is_their_place_in_all() {
    let names: Vec<&str> = LossKind::ALL.iter().map(|k| k.as_str()).collect();
    assert_eq!(
        names,
        ["clash", "deleteWon", "insertLost", "retiredWriter", "typeClash", "arrayClash"],
        "the names of the kinds are what a client reads"
    );
    for (n, kind) in LossKind::ALL.iter().enumerate() {
        assert_eq!(kind.index(), n, "{kind:?}");
    }
}

#[test]
fn a_value_that_was_cut_says_so_and_one_that_was_not_does_not() {
    assert!(LossValue::CutValue(Value::String("ab".into())).is_cut(), "a cut value says so");
    assert!(LossValue::CutFields(vec![p("a")]).is_cut(), "cut fields say so");
    assert!(!LossValue::Value(Value::String("ab".into())).is_cut());
    assert!(!LossValue::Fields(vec![p("a")]).is_cut());
    assert!(!LossValue::Total(Num::Int64(1)).is_cut());
}
