//! The array forms of the accumulators of ADR-262: `$firstN`, `$lastN`, `$maxN`
//! and `$minN` over an array (`{$maxN: {n: 2, input: [1, 3, 2, null]}}` is
//! `[3, 2]`), and `$stdDevPop` / `$stdDevSamp` over an array or a list of
//! operands (`{$stdDevPop: [1, 2, 3, 4]}` is `1.118033988749895`).
//!
//! They answer what the group accumulator of the same name answers over the same
//! values: `$firstN` and `$lastN` keep a `null`, `$maxN` and `$minN` skip it, and
//! `$stdDev*` counts only the numbers and answers a double. `n` is read for each
//! document, as any expression is.
//!
//! The reference's answers for these are confirmed for an array operand with
//! numbers, a null, and (for `$stdDev*`) a string among them. A `null` or
//! missing `input` is answered `null` here, and an `input` that is not an array
//! is refused; neither was checked against the reference (deviations.md).

use bson::Bson;
use kimmy_core::{Error, Result};

use super::{Declared, Expr, Scope, type_name};
use crate::accumulators::{self, Kind};

/// One of the array forms.
#[derive(Clone, Debug, PartialEq)]
pub enum ArrayAcc {
    /// `{$firstN: {input, n}}` and its three siblings.
    N { kind: Kind, input: Expr, n: Expr },
    /// `$stdDevPop` / `$stdDevSamp`: one operand that is an array (or a number),
    /// or several operands.
    StdDev { sample: bool, operands: Vec<Expr> },
}

/// Whether `name` is one of the operators of this module.
pub(super) fn handles(name: &str) -> bool {
    matches!(name, "$firstN" | "$lastN" | "$maxN" | "$minN" | "$stdDevPop" | "$stdDevSamp")
}

impl ArrayAcc {
    pub(super) fn children(&self) -> Vec<&Expr> {
        match self {
            ArrayAcc::N { input, n, .. } => vec![input, n],
            ArrayAcc::StdDev { operands, .. } => operands.iter().collect(),
        }
    }

    pub(super) fn parse(name: &str, raw: &Bson, declared: &mut Declared) -> Result<Expr> {
        let node = match name {
            "$stdDevPop" | "$stdDevSamp" => {
                let operands = match raw {
                    Bson::Array(items) => items
                        .iter()
                        .map(|item| Expr::parse_in(item, declared))
                        .collect::<Result<Vec<_>>>()?,
                    single => vec![Expr::parse_in(single, declared)?],
                };
                ArrayAcc::StdDev { sample: name == "$stdDevSamp", operands }
            }
            _ => {
                let kind = match name {
                    "$firstN" => Kind::FirstN,
                    "$lastN" => Kind::LastN,
                    "$maxN" => Kind::MaxN,
                    _ => Kind::MinN,
                };
                let spec = Expr::named_spec(name, raw, &["input", "n"])?;
                ArrayAcc::N {
                    kind,
                    input: Expr::parse_in(Expr::required(name, spec, "input")?, declared)?,
                    n: Expr::parse_in(Expr::required(name, spec, "n")?, declared)?,
                }
            }
        };
        Ok(Expr::ArrayAcc(Box::new(node)))
    }

    pub(super) fn eval(&self, scope: &Scope<'_>) -> Result<Bson> {
        match self {
            ArrayAcc::StdDev { sample, operands } => {
                // One operand that is an array is its elements; any other single
                // operand is itself; several are each themselves.
                let values: Vec<Bson> = match operands.as_slice() {
                    [one] => match one.eval_in(scope)? {
                        Bson::Array(items) => items,
                        other => vec![other],
                    },
                    many => many.iter().map(|e| e.eval_in(scope)).collect::<Result<_>>()?,
                };
                Ok(accumulators::std_dev_of(&values, *sample))
            }
            ArrayAcc::N { kind, input, n } => {
                let name = match kind {
                    Kind::FirstN => "$firstN",
                    Kind::LastN => "$lastN",
                    Kind::MaxN => "$maxN",
                    _ => "$minN",
                };
                let items = match input.eval_in(scope)? {
                    Bson::Array(items) => items,
                    Bson::Null | Bson::Undefined => return Ok(Bson::Null),
                    other => {
                        return Err(Error::InvalidQuery(format!(
                            "{name} requires `input` to be an array, found {}",
                            type_name(&other)
                        )));
                    }
                };
                let n = accumulators::read_n(name, &n.eval_in(scope)?)?;
                let picked = accumulators::n_of_array(*kind, &items, n);
                // At most the input's elements, and the input is itself bounded; the
                // check is one pass over the result, for the array limit to hold here too.
                if let Bson::Array(picked) = &picked {
                    super::check_array_within(
                        "the array the N accumulator form builds",
                        picked,
                        super::MAX_ARRAY_BYTES,
                    )?;
                }
                Ok(picked)
            }
        }
    }
}
