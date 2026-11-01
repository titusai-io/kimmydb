//! `$fill` (ADR-264): null and missing values filled from a constant, an expression, or the
//! neighbouring documents.
//!
//! ```text
//! {$fill: {
//!   partitionBy: <expression>, | partitionByFields: [<path>, ...],   // at most one
//!   sortBy: {<field>: 1 | -1, ...},
//!   output: {<field>: {value: <expression>} | {method: "locf" | "linear"}, ...}
//! }}
//! ```
//!
//! `locf` and `linear` are the window operators `$locf` and `$linearFill` of
//! [`crate::window`], run as a `$setWindowFields` over the partition (so the documents leave
//! sorted, partition by partition, as that stage leaves them). `value` fills a field that is
//! null or missing with the expression, read from the document as the fill methods left it. A
//! `$fill` with only `value` fields buffers nothing and keeps the order of its input.

use bson::{Bson, Document};
use kimmy_core::{Error, Result, path};

use crate::aggregate::Limits;
use crate::expr::{Binding, Expr, Scope, type_name};
use crate::window::{
    Func, Output, Partition, WindowSpec, apply as apply_window, check_output_name, parse_sort_by,
};

fn bad<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidQuery(message.into()))
}

/// A parsed `$fill`.
#[derive(Clone, Debug)]
pub struct FillSpec {
    /// The `locf` and `linear` fields, as the window stage that computes them.
    pub window: Option<WindowSpec>,
    /// The `value` fields.
    pub values: Vec<(String, Expr)>,
}

pub fn parse(spec: &Document, vars: &[String]) -> Result<FillSpec> {
    for key in spec.keys() {
        if !matches!(key.as_str(), "partitionBy" | "partitionByFields" | "sortBy" | "output") {
            return bad(format!(
                "$fill does not take `{key}`; its fields are partitionBy, partitionByFields, \
                 sortBy and output"
            ));
        }
    }
    if spec.contains_key("partitionBy") && spec.contains_key("partitionByFields") {
        return bad("$fill takes one of `partitionBy` and `partitionByFields`, found both");
    }
    let partition = if let Some(value) = spec.get("partitionBy") {
        // The reference takes an expression object here, not a field name or a number.
        if !matches!(value, Bson::Document(_)) {
            return bad(format!(
                "$fill `partitionBy` must be an object expression, found {}; to partition by a \
                 field use `partitionByFields`",
                type_name(value)
            ));
        }
        match Expr::parse_with_vars(value, vars)? {
            Expr::Literal(_) => Partition::Whole,
            expr => Partition::Expr(expr),
        }
    } else if let Some(value) = spec.get("partitionByFields") {
        let Bson::Array(items) = value else {
            return bad(format!(
                "$fill `partitionByFields` must be an array of field paths, found {}",
                type_name(value)
            ));
        };
        let mut fields = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Bson::String(f)
                    if !f.is_empty()
                        && !f.starts_with('$')
                        && f.split('.').all(|s| !s.is_empty()) =>
                {
                    fields.push(f.clone());
                }
                other => {
                    return bad(format!(
                        "$fill `partitionByFields` holds field paths without a leading `$`, found {}",
                        match other {
                            Bson::String(s) => format!("{s:?}"),
                            other => type_name(other).to_string(),
                        }
                    ));
                }
            }
        }
        if fields.is_empty() { Partition::Whole } else { Partition::Fields(fields) }
    } else {
        Partition::Whole
    };
    let sort = match spec.get("sortBy") {
        None => Vec::new(),
        Some(value) => parse_sort_by("$fill", value)?,
    };
    let output = match spec.get("output") {
        Some(Bson::Document(d)) if !d.is_empty() => d,
        Some(Bson::Document(_)) => return bad("$fill `output` must name at least one field"),
        Some(other) => {
            return bad(format!("$fill `output` must be a document, found {}", type_name(other)));
        }
        None => return bad("$fill needs `output`: {<field>: {value: ..} | {method: ..}}"),
    };
    let mut methods: Vec<Output> = Vec::new();
    let mut values: Vec<(String, Expr)> = Vec::new();
    for (name, how) in output {
        check_output_name("$fill", name)?;
        let Some(how) = how.as_document() else {
            return bad(format!(
                "$fill output `{name}` must be {{value: ..}} or {{method: ..}}, found {}",
                type_name(how)
            ));
        };
        for key in how.keys() {
            if !matches!(key.as_str(), "value" | "method") {
                return bad(format!("$fill output `{name}` does not take `{key}`"));
            }
        }
        match (how.get("value"), how.get("method")) {
            (Some(_), Some(_)) => {
                return bad(format!(
                    "$fill output `{name}` takes one of `value` and `method`, found both"
                ));
            }
            (None, None) => {
                return bad(format!("$fill output `{name}` needs `value` or `method`"));
            }
            (Some(value), None) => {
                values.push((name.clone(), Expr::parse_with_vars(value, vars)?));
            }
            (None, Some(Bson::String(method))) => {
                let field = Expr::parse_with_vars(&Bson::String(format!("${name}")), vars)?;
                let func = match method.as_str() {
                    // `locf` carries forward in the order the documents arrive when there is
                    // no `sortBy`.
                    "locf" => Func::Locf(field),
                    "linear" => {
                        if sort.len() != 1 {
                            return bad(
                                "$fill method \"linear\" requires a `sortBy` of exactly one field",
                            );
                        }
                        Func::LinearFill(field)
                    }
                    other => {
                        return bad(format!(
                            "$fill method is \"locf\" or \"linear\", found {other:?}"
                        ));
                    }
                };
                methods.push(Output { name: name.clone(), func });
            }
            (None, Some(other)) => {
                return bad(format!(
                    "$fill method must be \"locf\" or \"linear\", found {}",
                    type_name(other)
                ));
            }
        }
    }
    let window = (!methods.is_empty()).then_some(WindowSpec {
        stage: "$fill",
        partition,
        sort,
        outputs: methods,
    });
    Ok(FillSpec { window, values })
}

/// Run a parsed `$fill`.
pub fn apply(
    spec: &FillSpec,
    input: Vec<Document>,
    limits: &Limits,
    vars: &[Binding<'_>],
) -> Result<Vec<Document>> {
    let mut docs = match &spec.window {
        Some(window) => apply_window(window, input, limits, vars)?,
        None => input,
    };
    if spec.values.is_empty() {
        return Ok(docs);
    }
    for doc in &mut docs {
        // Every value is read from the document before any is written, as `$addFields` does.
        let mut fills: Vec<(&str, Bson)> = Vec::new();
        for (name, expr) in &spec.values {
            let empty =
                matches!(path::resolve(doc, name).into_iter().next(), None | Some(Bson::Null));
            if empty && let Some(value) = expr.eval_field(&Scope::with_bindings(doc, vars))? {
                fills.push((name, value));
            }
        }
        for (name, value) in fills {
            path::set(doc, name, value)
                .map_err(|e| Error::InvalidQuery(format!("cannot set {name:?}: {e}")))?;
        }
    }
    Ok(docs)
}
