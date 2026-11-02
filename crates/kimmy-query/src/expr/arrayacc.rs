//! The array forms of the accumulators of ADR-262: `$firstN`, `$lastN`, `$maxN`
//! and `$minN` over an array (`{$maxN: {n: 2, input: [1, 3, 2, null]}}` is
//! `[3, 2]`), and `$stdDevPop` / `$stdDevSamp` over an array or a list of
//! operands (`{$stdDevPop: [1, 2, 3, 4]}` is `1.118033988749895`); and `$sum`, `$avg`,
//! `$min` and `$max` as expressions, over an array or a list of operands (`{$sum: [1, 2,
//! 3]}` and `{$sum: "$prices"}` are both a total).
//!
//! They answer what the group accumulator of the same name answers over the same
//! values: `$firstN` and `$lastN` keep a `null`, `$maxN` and `$minN` skip it, and
//! `$stdDev*` counts only the numbers and answers a double. `n` is read for each
//! document, as any expression is. `$sum` and `$avg` count only the numbers and `$min` and
//! `$max` skip a null, with the widths, the decimal rule and the order of the
//! accumulators (ADR-252, ADR-268), because they are the same code: `Total`, `Mean` and
//! `aggregate::keep_extreme`. One operand that is an array is its elements; an element that
//! is itself an array is a value (not a number, so not summed, and ordered by `$min` and
//! `$max` as an array); any other single operand is itself.
//!
//! An `input` that is not an array is refused, `null` and missing included, as
//! the reference refuses it (error 5788200); `n` is read before `input`.

use bson::Bson;
use kimmy_core::{Error, Result};

use super::{Declared, Expr, Mean, Scope, Total, type_name};
use crate::accumulators::{self, Kind};
use crate::aggregate::keep_extreme;

/// One of the array forms.
#[derive(Clone, Debug, PartialEq)]
pub enum ArrayAcc {
    /// `{$firstN: {input, n}}` and its three siblings.
    N { kind: Kind, input: Expr, n: Expr },
    /// `$stdDevPop` / `$stdDevSamp`: one operand that is an array (or a number),
    /// or several operands.
    StdDev { sample: bool, operands: Vec<Expr> },
    /// `$sum`, `$avg`, `$min` or `$max`: one operand that is an array (or a value), or
    /// several operands.
    Fold { fold: Fold, operands: Vec<Expr> },
}

/// Which of the four.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fold {
    Sum,
    Avg,
    Min,
    Max,
}

/// Whether `name` is one of the operators of this module.
pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "$firstN"
            | "$lastN"
            | "$maxN"
            | "$minN"
            | "$stdDevPop"
            | "$stdDevSamp"
            | "$sum"
            | "$avg"
            | "$min"
            | "$max"
    )
}

impl ArrayAcc {
    pub(super) fn children(&self) -> Vec<&Expr> {
        match self {
            ArrayAcc::N { input, n, .. } => vec![input, n],
            ArrayAcc::StdDev { operands, .. } | ArrayAcc::Fold { operands, .. } => {
                operands.iter().collect()
            }
        }
    }

    pub(super) fn parse(name: &str, raw: &Bson, declared: &mut Declared) -> Result<Expr> {
        let node = match name {
            "$stdDevPop" | "$stdDevSamp" => {
                let operands = parse_operands(raw, declared)?;
                ArrayAcc::StdDev { sample: name == "$stdDevSamp", operands }
            }
            "$sum" | "$avg" | "$min" | "$max" => {
                let fold = match name {
                    "$sum" => Fold::Sum,
                    "$avg" => Fold::Avg,
                    "$min" => Fold::Min,
                    _ => Fold::Max,
                };
                ArrayAcc::Fold { fold, operands: parse_operands(raw, declared)? }
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
                let values = operand_values(operands, scope)?;
                super::charge_elements(&values)?;
                Ok(accumulators::std_dev_of(&values, *sample))
            }
            ArrayAcc::Fold { fold, operands } => {
                let values = operand_values(operands, scope)?;
                // A pass over the values, and a comparison of them for `$min` and `$max`.
                super::charge_elements(&values)?;
                Ok(fold_of(*fold, values))
            }
            ArrayAcc::N { kind, input, n } => {
                let name = match kind {
                    Kind::FirstN => "$firstN",
                    Kind::LastN => "$lastN",
                    Kind::MaxN => "$maxN",
                    _ => "$minN",
                };
                // `n` is read first: a bad `n` is the error beside a bad `input`.
                let n = accumulators::read_n(name, &n.eval_in(scope)?)?;
                let items = match input.eval_in(scope)? {
                    Bson::Array(items) => items,
                    other => {
                        return Err(Error::InvalidQuery(format!(
                            "{name} requires `input` to be an array, found {}",
                            type_name(&other)
                        )));
                    }
                };
                // A pass over the input, and a comparison of its elements for `$maxN` and `$minN`.
                super::charge_elements(&items)?;
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

/// The operands of `$stdDevPop`, `$sum` and their kin: an array is each of its items, anything
/// else is the one operand.
fn parse_operands(raw: &Bson, declared: &mut Declared) -> Result<Vec<Expr>> {
    match raw {
        Bson::Array(items) => {
            items.iter().map(|item| Expr::parse_in(item, declared)).collect::<Result<Vec<_>>>()
        }
        single => Ok(vec![Expr::parse_in(single, declared)?]),
    }
}

/// The values the operands stand for. One operand that is an array is its elements; any other
/// single operand is itself; several are each themselves.
fn operand_values(operands: &[Expr], scope: &Scope<'_>) -> Result<Vec<Bson>> {
    match operands {
        [one] => Ok(match one.eval_in(scope)? {
            Bson::Array(items) => items,
            other => vec![other],
        }),
        many => many.iter().map(|e| e.eval_in(scope)).collect(),
    }
}

/// The total, mean, least or greatest of `values`, as the group accumulator answers it.
fn fold_of(fold: Fold, values: Vec<Bson>) -> Bson {
    match fold {
        Fold::Sum => {
            let mut total = Total::default();
            values.iter().for_each(|v| total.add(v));
            total.to_bson()
        }
        Fold::Avg => {
            let mut mean = Mean::default();
            values.iter().for_each(|v| mean.add(v));
            mean.to_bson()
        }
        Fold::Min | Fold::Max => {
            let mut current = None;
            for v in values {
                keep_extreme(&mut current, v, fold == Fold::Max);
            }
            current.unwrap_or(Bson::Null)
        }
    }
}
