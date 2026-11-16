//! Two more edges: the logarithm of a decimal `NaN` is a double (q7s4b 2686, 2687, 2705, 2706), and
//! date arithmetic stays in integers (q7s4g 232: `$toLong` of `$add [Date(0), Long(2^53 + 1)]`).

use super::common::run;
use bson::{Bson, doc};

fn eval(expr: Bson) -> Result<Bson, String> {
    let out = run(&[doc! { "$project": { "_id": 0, "r": expr } }], vec![doc! { "_id": 1 }])?;
    Ok(out[0].get("r").cloned().unwrap())
}

fn decimal(text: &str) -> Bson {
    Bson::Decimal128(text.parse().expect("a decimal"))
}

#[test]
fn the_logarithm_of_a_decimal_nan_is_a_double_nan_and_the_other_functions_keep_their_type() {
    for op in ["$ln", "$log10"] {
        let got = eval(Bson::Document(doc! { op: [decimal("NaN")] })).unwrap();
        assert!(matches!(got, Bson::Double(d) if d.is_nan()), "{op}: {got:?}");
    }
    // `$sqrt` and `$exp` of a decimal `NaN` are a decimal `NaN` (2649, 2668), and a finite
    // decimal gives a decimal (2686, 2705).
    for op in ["$sqrt", "$exp"] {
        let got = eval(Bson::Document(doc! { op: [decimal("NaN")] })).unwrap();
        assert!(matches!(got, Bson::Decimal128(_)), "{op}: {got:?}");
    }
    for op in ["$ln", "$log10"] {
        let got = eval(Bson::Document(doc! { op: [decimal("1.5")] })).unwrap();
        assert!(matches!(got, Bson::Decimal128(_)), "{op}: {got:?}");
    }
}

#[test]
fn a_date_shifted_by_a_long_past_2_53_stays_exact() {
    // g:232
    let date = Bson::DateTime(bson::DateTime::from_millis(0));
    let got = eval(bson::bson!({ "$toLong": { "$add": [date, 9_007_199_254_740_993_i64] } }));
    assert_eq!(got, Ok(Bson::Int64(9_007_199_254_740_993)));
    // g:231: past the range of a long is refused.
    let date = Bson::DateTime(bson::DateTime::from_millis(1));
    eval(bson::bson!({ "$toLong": { "$add": [date, i64::MAX] } })).unwrap_err();
}
