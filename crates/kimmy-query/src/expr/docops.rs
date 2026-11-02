//! The document and type operators of ADR-260: `$getField`, `$setField`,
//! `$unsetField`, `$type`, and the merge `$mergeObjects` is (also the `$group`
//! accumulator of that name).
//!
//! These are the operators that must tell a **missing** value from a null one.
//! Everywhere else an expression reads a missing field as null (the array
//! operators' entry in `deviations.md`); here `$type` answers `"missing"`,
//! `$setField` with a missing value removes the field, and `$getField` of a
//! field the document lacks, or of an `input` that is missing or is not a
//! document, is missing, which a `$project`/`$addFields` leaves out. [`eval_opt`]
//! is the evaluation that keeps the distinction: `None` is a missing value.
//!
//! The field name of `$getField`, `$setField` and `$unsetField` is a constant,
//! read when the expression is parsed, as the reference reads it: a string or a
//! `$literal` of one. A field path or a computed name is refused.

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

use super::{
    Declared, DocBudget, Expr, MAX_ARRAY_BYTES, Op, Scope, bson_bytes, field_path, truthy,
    value_path,
};

/// A document or type operator.
#[derive(Clone, Debug, PartialEq)]
pub enum DocExpr {
    /// `{$getField: <name>}` or `{$getField: {field, input}}`; the shorthand reads
    /// `$$CURRENT`, which is `input: None`, and the named form must give `input`.
    GetField { field: String, input: Option<Expr> },
    /// `{$setField: {field, input, value}}`, and `$unsetField`, which is the same
    /// without a value (`value: None`).
    SetField { field: String, input: Expr, value: Option<Expr> },
    /// `{$type: <expr>}`.
    Type(Expr),
}

fn nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

/// The reference's name for a type, `$type`'s answer.
pub(super) fn bson_type_name(v: Option<&Bson>) -> &'static str {
    match v {
        None => "missing",
        Some(Bson::Double(_)) => "double",
        Some(Bson::String(_)) => "string",
        Some(Bson::Document(_)) => "object",
        Some(Bson::Array(_)) => "array",
        Some(Bson::Binary(_)) => "binData",
        Some(Bson::Undefined) => "undefined",
        Some(Bson::ObjectId(_)) => "objectId",
        Some(Bson::Boolean(_)) => "bool",
        Some(Bson::DateTime(_)) => "date",
        Some(Bson::Null) => "null",
        Some(Bson::RegularExpression(_)) => "regex",
        Some(Bson::DbPointer(_)) => "dbPointer",
        Some(Bson::JavaScriptCode(_)) => "javascript",
        Some(Bson::Symbol(_)) => "symbol",
        Some(Bson::JavaScriptCodeWithScope(_)) => "javascriptWithScope",
        Some(Bson::Int32(_)) => "int",
        Some(Bson::Timestamp(_)) => "timestamp",
        Some(Bson::Int64(_)) => "long",
        Some(Bson::Decimal128(_)) => "decimal",
        Some(Bson::MaxKey) => "maxKey",
        Some(Bson::MinKey) => "minKey",
    }
}

impl DocExpr {
    pub(super) fn children(&self) -> Vec<&Expr> {
        match self {
            DocExpr::GetField { input, .. } => input.iter().collect(),
            DocExpr::SetField { input, value, .. } => {
                std::iter::once(input).chain(value.as_ref()).collect()
            }
            DocExpr::Type(e) => vec![e],
        }
    }

    /// Whether it reads `$$CURRENT` without being written to: a `$getField`
    /// with no `input`.
    pub(super) fn reads_current(&self) -> bool {
        matches!(self, DocExpr::GetField { input: None, .. })
    }

    pub(super) fn parse(name: &str, raw: &Bson, declared: &mut Declared) -> Result<Expr> {
        let node = match name {
            "$type" => {
                let arg = match raw {
                    Bson::Array(items) if items.len() == 1 => &items[0],
                    Bson::Array(items) => {
                        return Err(Error::InvalidQuery(format!(
                            "$type takes exactly 1 argument, found {}",
                            items.len()
                        )));
                    }
                    single => single,
                };
                DocExpr::Type(Expr::parse_in(arg, declared)?)
            }
            "$getField" => match raw {
                // The named form is a document of plain names; one whose first
                // key is an operator (`{$literal: "$x"}`) is the field itself.
                Bson::Document(d) if d.keys().next().is_some_and(|k| !k.starts_with('$')) => {
                    let spec = Expr::named_spec(name, raw, &["field", "input"])?;
                    let field = Expr::required(name, spec, "field")?;
                    // The reference has no default for `input`.
                    let input = Expr::required(name, spec, "input")?;
                    DocExpr::GetField {
                        field: constant_field(name, field, declared)?,
                        input: Some(Expr::parse_in(input, declared)?),
                    }
                }
                Bson::Array(_) => {
                    return Err(Error::InvalidQuery(
                        "$getField takes a field name or {field, input}, not an array".into(),
                    ));
                }
                shorthand => DocExpr::GetField {
                    field: constant_field(name, shorthand, declared)?,
                    input: None,
                },
            },
            "$setField" | "$unsetField" => {
                let set = name == "$setField";
                let allowed: &[&str] =
                    if set { &["field", "input", "value"] } else { &["field", "input"] };
                let spec = Expr::named_spec(name, raw, allowed)?;
                let field = Expr::required(name, spec, "field")?;
                let input = Expr::parse_in(Expr::required(name, spec, "input")?, declared)?;
                let value = if set {
                    Some(Expr::parse_in(Expr::required(name, spec, "value")?, declared)?)
                } else {
                    None
                };
                DocExpr::SetField { field: constant_field(name, field, declared)?, input, value }
            }
            _ => unreachable!("dispatched by name"),
        };
        Ok(Expr::Doc(Box::new(node)))
    }

    /// The value, `None` when it is missing.
    pub(super) fn eval_opt(&self, scope: &Scope<'_>) -> Result<Option<Bson>> {
        match self {
            DocExpr::Type(e) => {
                let v = eval_opt(e, scope)?;
                Ok(Some(Bson::String(bson_type_name(v.as_ref()).to_string())))
            }
            DocExpr::GetField { field, input } => Ok(match input {
                None => scope.root().get(field).cloned(),
                Some(i) => match eval_opt(i, scope)? {
                    Some(Bson::Document(d)) => d.get(field).cloned(),
                    // Null answers null.
                    Some(v) if nullish(&v) => Some(Bson::Null),
                    // A missing input and one that is not a document are missing.
                    _ => None,
                },
            }),
            DocExpr::SetField { field, input, value } => {
                let op = if value.is_some() { "$setField" } else { "$unsetField" };
                let mut doc = match eval_opt(input, scope)? {
                    None => return Ok(Some(Bson::Null)),
                    Some(v) if nullish(&v) => return Ok(Some(Bson::Null)),
                    Some(Bson::Document(d)) => d,
                    Some(other) => {
                        return Err(Error::InvalidQuery(format!(
                            "{op} requires 'input' to evaluate to type Object, but got {}",
                            bson_type_name(Some(&other))
                        )));
                    }
                };
                let value = match value {
                    Some(v) => eval_opt(v, scope)?,
                    None => None,
                };
                set_field_within(&mut doc, field, value, MAX_ARRAY_BYTES)?;
                Ok(Some(Bson::Document(doc)))
            }
        }
    }
}

/// Set `field` of `doc` to `value`, or remove it when `value` is missing. A new
/// field goes last; an existing one keeps its place. The document `doc` becomes is
/// counted before the value is stored, and refused when it passes `max` bytes.
pub(super) fn set_field_within(
    doc: &mut Document,
    field: &str,
    value: Option<Bson>,
    max: usize,
) -> Result<()> {
    match value {
        Some(v) => {
            let mut budget = DocBudget::of_document("the document $setField builds", max, doc)?;
            budget.set(field, doc.get(field).map(bson_bytes), &v)?;
            doc.insert(field, v);
        }
        None => {
            doc.remove(field);
        }
    }
    Ok(())
}

/// The field name of `$getField`, `$setField` or `$unsetField`, which must be a
/// constant string. A field path, a computed name and a value of another type
/// are refused as the reference refuses them.
fn constant_field(op: &str, raw: &Bson, declared: &mut Declared) -> Result<String> {
    if let Bson::String(path) = raw
        && path.starts_with('$')
        && path.len() > 1
    {
        return Err(Error::InvalidQuery(format!(
            "{op}: '{path}' is a field path reference which is not allowed in this context. \
             Did you mean {{$literal: '{path}'}}?"
        )));
    }
    let constant = match Expr::parse_in(raw, declared)? {
        Expr::Literal(v) => v,
        Expr::Op(Op::Literal, args) => match args.as_slice() {
            [Expr::Literal(v)] => v.clone(),
            _ => unreachable!("$literal holds one constant"),
        },
        _ => {
            return Err(Error::InvalidQuery(format!(
                "{op} requires 'field' to evaluate to a constant, but got a non-constant argument"
            )));
        }
    };
    match constant {
        Bson::String(name) => Ok(name),
        other => Err(Error::InvalidQuery(format!(
            "{op} requires 'field' to evaluate to type String, but got {}",
            bson_type_name(Some(&other))
        ))),
    }
}

/// Evaluate keeping a missing value apart from null: a field path that reaches
/// nothing, `$$REMOVE`, `$getField` of an absent field, and a `$cond`,
/// `$switch` or `$let` that chooses one.
pub(super) fn eval_opt(expr: &Expr, scope: &Scope<'_>) -> Result<Option<Bson>> {
    match expr {
        Expr::Field(p) => Ok(field_path(scope.root(), p)),
        Expr::Var { name, path: Some(p) } if name != "REMOVE" => {
            // Read through the ordinary evaluation for the variable, then walk
            // the path, so that an unbound name fails as it does there.
            let whole = Expr::Var { name: name.clone(), path: None }.eval_in(scope)?;
            Ok(value_path(&whole, p))
        }
        Expr::Doc(d) => d.eval_opt(scope),
        Expr::Op(Op::Cond, args) => match args.as_slice() {
            [cond, then, otherwise] => {
                let chosen = if truthy(&cond.eval_in(scope)?) { then } else { otherwise };
                eval_opt(chosen, scope)
            }
            _ => unreachable!("arity checked at parse"),
        },
        Expr::Switch { branches, default } => {
            for (case, then) in branches {
                if truthy(&case.eval_in(scope)?) {
                    return eval_opt(then, scope);
                }
            }
            match default {
                Some(d) => eval_opt(d, scope),
                None => expr.eval_in(scope).map(Some),
            }
        }
        Expr::Let { .. } => expr.eval_field(scope),
        other => other.eval_field(scope),
    }
}

// ---------------------------------------------------------------------------
// $mergeObjects
// ---------------------------------------------------------------------------

/// Merge `value` into `into`: null and a missing field are skipped, a document's
/// fields are set in turn, so a later value wins and an existing key keeps its
/// place. Anything else is refused. `budget` counts `into` as it grows, and
/// refuses a field before it is copied when the document would pass the limit.
pub(crate) fn merge_into(into: &mut Document, budget: &mut DocBudget, value: &Bson) -> Result<()> {
    match value {
        v if nullish(v) => Ok(()),
        Bson::Document(d) => {
            for (k, v) in d {
                budget.set(k, into.get(k).map(bson_bytes), v)?;
                budget.account_held(into.len(), k, into.get(k), v);
                into.insert(k.clone(), v.clone());
            }
            Ok(())
        }
        other => Err(Error::InvalidQuery(format!(
            "$mergeObjects requires object inputs, but input {other} is of type {}",
            bson_type_name(Some(other))
        ))),
    }
}

/// The budget of the document `$mergeObjects` builds, empty.
pub(crate) fn merge_budget(max: usize) -> DocBudget {
    DocBudget::new("the document $mergeObjects builds", max)
}

pub(super) fn merge_objects(args: &[Bson]) -> Result<Bson> {
    merge_objects_within(args, MAX_ARRAY_BYTES)
}

/// [`merge_objects`] within `max` bytes.
pub(super) fn merge_objects_within(args: &[Bson], max: usize) -> Result<Bson> {
    let mut out = Document::new();
    let mut budget = merge_budget(max);
    for a in args {
        merge_into(&mut out, &mut budget, a)?;
    }
    Ok(Bson::Document(out))
}

pub(super) fn is_number(arg: &Bson) -> Bson {
    Bson::Boolean(matches!(
        arg,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    ))
}
