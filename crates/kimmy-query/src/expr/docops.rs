//! The document and type operators of ADR-260: `$getField`, `$setField`,
//! `$unsetField`, `$type`, and the merge `$mergeObjects` is (also the `$group`
//! accumulator of that name).
//!
//! These are the operators that must tell a **missing** value from a null one.
//! Everywhere else an expression reads a missing field as null (the array
//! operators' entry in `deviations.md`); here `$type` answers `"missing"`,
//! `$setField` with a missing value removes the field, and `$getField` of a
//! field the document lacks is missing, which a `$project`/`$addFields`
//! leaves out. [`eval_opt`] is the evaluation that keeps the distinction:
//! `None` is a missing value.

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

use super::{Declared, Expr, Op, Scope, field_path, truthy, value_path};

/// A document or type operator.
#[derive(Clone, Debug, PartialEq)]
pub enum DocExpr {
    /// `{$getField: <field>}` or `{$getField: {field, input}}`; `input` is
    /// `$$CURRENT` when absent.
    GetField { field: Expr, input: Option<Expr> },
    /// `{$setField: {field, input, value}}`, and `$unsetField`, which is the same
    /// with `$$REMOVE` for the value (`remove`).
    SetField { field: Expr, input: Expr, value: Option<Expr> },
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
            DocExpr::GetField { field, input } => {
                std::iter::once(field).chain(input.as_ref()).collect()
            }
            DocExpr::SetField { field, input, value } => {
                [field, input].into_iter().chain(value.as_ref()).collect()
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
                    DocExpr::GetField {
                        field: Expr::parse_in(Expr::required(name, spec, "field")?, declared)?,
                        input: spec
                            .get("input")
                            .map(|i| Expr::parse_in(i, declared))
                            .transpose()?,
                    }
                }
                Bson::Array(_) => {
                    return Err(Error::InvalidQuery(
                        "$getField takes a field name or {field, input}, not an array".into(),
                    ));
                }
                shorthand => {
                    DocExpr::GetField { field: Expr::parse_in(shorthand, declared)?, input: None }
                }
            },
            "$setField" | "$unsetField" => {
                let set = name == "$setField";
                let allowed: &[&str] =
                    if set { &["field", "input", "value"] } else { &["field", "input"] };
                let spec = Expr::named_spec(name, raw, allowed)?;
                let mut get = |key: &str| -> Result<Expr> {
                    Expr::parse_in(Expr::required(name, spec, key)?, declared)
                };
                let (field, input) = (get("field")?, get("input")?);
                let value = if set { Some(get("value")?) } else { None };
                DocExpr::SetField { field, input, value }
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
            DocExpr::GetField { field, input } => {
                let name = field_name("$getField", eval_opt(field, scope)?)?;
                let doc = match input {
                    None => scope.root().clone(),
                    Some(i) => match eval_opt(i, scope)? {
                        // Null and a missing input answer null.
                        None => return Ok(Some(Bson::Null)),
                        Some(v) if nullish(&v) => return Ok(Some(Bson::Null)),
                        Some(Bson::Document(d)) => d,
                        Some(other) => {
                            return Err(Error::InvalidQuery(format!(
                                "$getField requires 'input' to evaluate to type Object, but got {}",
                                bson_type_name(Some(&other))
                            )));
                        }
                    },
                };
                Ok(doc.get(&name).cloned())
            }
            DocExpr::SetField { field, input, value } => {
                let op = if value.is_some() { "$setField" } else { "$unsetField" };
                let name = field_name(op, eval_opt(field, scope)?)?;
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
                match value {
                    // A new field goes last; an existing one keeps its place.
                    Some(v) => {
                        doc.insert(name, v);
                    }
                    None => {
                        doc.remove(&name);
                    }
                }
                Ok(Some(Bson::Document(doc)))
            }
        }
    }
}

fn field_name(op: &str, v: Option<Bson>) -> Result<String> {
    match v {
        Some(Bson::String(s)) => Ok(s),
        other => Err(Error::InvalidQuery(format!(
            "{op} requires 'field' to evaluate to type String, but got {}",
            bson_type_name(other.as_ref())
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
/// place. Anything else is refused.
pub(crate) fn merge_into(into: &mut Document, value: &Bson) -> Result<()> {
    match value {
        v if nullish(v) => Ok(()),
        Bson::Document(d) => {
            for (k, v) in d {
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

pub(super) fn merge_objects(args: &[Bson]) -> Result<Bson> {
    let mut out = Document::new();
    for a in args {
        merge_into(&mut out, a)?;
    }
    Ok(Bson::Document(out))
}

pub(super) fn is_number(arg: &Bson) -> Bson {
    Bson::Boolean(matches!(
        arg,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    ))
}
