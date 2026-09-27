//! `unsupported operator "…"` puts an operator in its quotes and nothing else.
//!
//! query-language.md gives the template with an operator in it,
//! `unsupported operator "$typo"`, and a client matches or logs that slot. A
//! partial index's `$or` refusal once filled it with a whole sentence of
//! explanation, as did an unknown expression operator and a system variable,
//! so what a client read out of the quotes was not an operator. The reason,
//! where there is one, now follows the quotes.

use bson::{Bson, doc};
use kimmy_core::Error;
use kimmy_core::partial::PartialFilter;
use kimmy_query::{Expr, filter, update};

/// The operator in the message's quotes, and whether it looks like one:
/// `$` or `$$`, then word characters, and nothing else.
fn slot(err: &Error) -> String {
    let message = err.to_string();
    let rest = message
        .strip_prefix("unsupported operator \"")
        .unwrap_or_else(|| panic!("not the template: {message}"));
    let operator = &rest[..rest.find('"').expect("the closing quote")];
    let bare = operator.trim_start_matches('$');
    assert!(
        operator.starts_with('$')
            && !bare.is_empty()
            && bare.chars().all(|c| c.is_alphanumeric() || c == '_'),
        "the slot holds {operator:?}, not an operator: {message}"
    );
    assert!(
        matches!(err, Error::UnsupportedOperator { .. }),
        "the template from another variant: {err:?}"
    );
    operator.to_string()
}

#[test]
fn a_partial_filters_refusals_name_the_operator_and_say_why_after_it() {
    let or = PartialFilter::parse(&doc! { "$or": [{ "a": 1 }, { "a": 2 }] }).unwrap_err();
    assert_eq!(slot(&or), "$or");
    assert_eq!(
        or.to_string(),
        "unsupported operator \"$or\": it cannot appear in a partialFilterExpression: only a \
         conjunction of per-field predicates is allowed, so containment stays decidable"
    );
    for (filter, operator) in [
        (doc! { "a": { "$ne": 1 } }, "$ne"),
        (doc! { "a": { "$in": [1] } }, "$in"),
        (doc! { "a": { "$regex": "^f" } }, "$regex"),
        (doc! { "a": { "$exists": false } }, "$exists"),
    ] {
        let err = PartialFilter::parse(&filter).unwrap_err();
        assert_eq!(slot(&err), operator);
        assert!(err.to_string().contains("partialFilterExpression for \"a\""), "{err}");
    }
}

#[test]
fn an_expressions_refusals_name_the_operator() {
    let unknown = Expr::parse(&Bson::Document(doc! { "$toDecimal": "$a" })).unwrap_err();
    assert_eq!(slot(&unknown), "$toDecimal");
    assert_eq!(
        unknown.to_string(),
        "unsupported operator \"$toDecimal\": not an expression operator"
    );
    let variable = Expr::parse(&Bson::String("$$NOW".into())).unwrap_err();
    assert_eq!(slot(&variable), "$$NOW");
}

#[test]
fn a_filters_and_an_updates_refusals_are_the_bare_template() {
    let err = filter::parse(&doc! { "a": { "$nope": 1 } }).unwrap_err();
    assert_eq!(err.to_string(), "unsupported operator \"$nope\"");
    let err = update::parse(&doc! { "$nope": { "a": 1 } }).unwrap_err();
    assert_eq!(slot(&err), "$nope");
}
