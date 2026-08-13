//! Computed expressions.
//!
//! An expression turns a document into a value. Before this module the only
//! expressions were a field path (`"$qty"`) and a literal, which meant a
//! pipeline could filter, group and join but could not *derive* — the single
//! largest gap against MongoDB's aggregation surface.
//!
//! # The shape
//!
//! [`Expr`] is a tree. [`Expr::parse`] builds it from BSON once and
//! [`Expr::eval`] walks it per document, matching how [`crate::filter`] already
//! works. Because `$group`'s `_id` and every accumulator argument were already
//! typed as `Expr`, they gain the whole operator set by construction rather
//! than by being taught about it separately.
//!
//! # How a value is read
//!
//! MongoDB's convention, followed here:
//!
//! - a string starting with `$` is a **field path**, and `$$` is not supported;
//! - a document whose **first key starts with `$`** is an **operator**, and its
//!   keys may not mix operator and plain names;
//! - any other document is an **object expression** — its values are
//!   expressions and the result is a document. This is what makes a compound
//!   `$group` key work;
//! - anything else is a literal.
//!
//! That third rule is a **behaviour change**: `{_id: {a: "$x"}}` used to be a
//! constant document, so every input landed in one bucket. It now computes.
//! [`Op::Literal`] is the escape hatch, and exists because the rule needs one —
//! without it there is no way to produce the string `"$x"`.
//!
//! # Numbers
//!
//! Integer arithmetic is done in `i64` and only falls to `f64` when an operand
//! is a double or the operation overflows. Accumulating in `f64` and casting
//! back — what `$sum` did before this module — silently loses precision above
//! 2^53, which is the same reasoning as ADR-002 and as `number_to_bson` at the
//! HTTP edge.
//!
//! **Integer results are always `Int64`.** MongoDB narrows to `Int32` when both
//! operands were `Int32`; a third case buys accuracy on `$type` that nothing
//! here needs, and `$sum` has always returned `Int64`.
//!
//! # Errors versus null
//!
//! Null propagates, type violations refuse. `{$add: ["$missing", 1]}` is null
//! because a missing field is null and adding to null is null; `{$add: ["a",
//! 1]}` is an error because a string is not a number. Returning null for both
//! would make a typo and a type error indistinguishable in the output.

use std::cmp::Ordering;

use bson::{Bson, Document};
use kimmy_core::{Error, Result, canonical_cmp, path};

// ---------------------------------------------------------------------------
// The operator set
// ---------------------------------------------------------------------------

/// An operator taking a positional argument list.
///
/// Operators whose arguments are *named* rather than positional — `$switch`
/// and `$dateToString` — are variants of [`Expr`] instead, because forcing
/// them through a list would lose the names the user wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Mod,
    // Strings
    Concat,
    ToUpper,
    ToLower,
    Substr,
    Split,
    StrLenCp,
    // Conditional
    Cond,
    IfNull,
    // Comparison
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Cmp,
    // Boolean
    And,
    Or,
    Not,
    // Dates
    Year,
    Month,
    DayOfMonth,
    Hour,
    Minute,
    Second,
    // Escape
    Literal,
}

/// How many arguments an operator accepts.
#[derive(Clone, Copy, Debug)]
enum Arity {
    Exact(usize),
    AtLeast(usize),
}

impl Op {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "$add" => Op::Add,
            "$subtract" => Op::Subtract,
            "$multiply" => Op::Multiply,
            "$divide" => Op::Divide,
            "$mod" => Op::Mod,
            "$concat" => Op::Concat,
            "$toUpper" => Op::ToUpper,
            "$toLower" => Op::ToLower,
            "$substr" | "$substrCP" => Op::Substr,
            "$split" => Op::Split,
            "$strLenCP" => Op::StrLenCp,
            "$cond" => Op::Cond,
            "$ifNull" => Op::IfNull,
            "$eq" => Op::Eq,
            "$ne" => Op::Ne,
            "$gt" => Op::Gt,
            "$gte" => Op::Gte,
            "$lt" => Op::Lt,
            "$lte" => Op::Lte,
            "$cmp" => Op::Cmp,
            "$and" => Op::And,
            "$or" => Op::Or,
            "$not" => Op::Not,
            "$year" => Op::Year,
            "$month" => Op::Month,
            "$dayOfMonth" => Op::DayOfMonth,
            "$hour" => Op::Hour,
            "$minute" => Op::Minute,
            "$second" => Op::Second,
            "$literal" => Op::Literal,
            _ => return None,
        })
    }

    /// The name as written, for error messages.
    pub fn name(self) -> &'static str {
        match self {
            Op::Add => "$add",
            Op::Subtract => "$subtract",
            Op::Multiply => "$multiply",
            Op::Divide => "$divide",
            Op::Mod => "$mod",
            Op::Concat => "$concat",
            Op::ToUpper => "$toUpper",
            Op::ToLower => "$toLower",
            Op::Substr => "$substr",
            Op::Split => "$split",
            Op::StrLenCp => "$strLenCP",
            Op::Cond => "$cond",
            Op::IfNull => "$ifNull",
            Op::Eq => "$eq",
            Op::Ne => "$ne",
            Op::Gt => "$gt",
            Op::Gte => "$gte",
            Op::Lt => "$lt",
            Op::Lte => "$lte",
            Op::Cmp => "$cmp",
            Op::And => "$and",
            Op::Or => "$or",
            Op::Not => "$not",
            Op::Year => "$year",
            Op::Month => "$month",
            Op::DayOfMonth => "$dayOfMonth",
            Op::Hour => "$hour",
            Op::Minute => "$minute",
            Op::Second => "$second",
            Op::Literal => "$literal",
        }
    }

    fn arity(self) -> Arity {
        match self {
            Op::Add | Op::Multiply | Op::Concat | Op::And | Op::Or => Arity::AtLeast(1),
            Op::Subtract
            | Op::Divide
            | Op::Mod
            | Op::Split
            | Op::IfNull
            | Op::Eq
            | Op::Ne
            | Op::Gt
            | Op::Gte
            | Op::Lt
            | Op::Lte
            | Op::Cmp => Arity::Exact(2),
            Op::Substr => Arity::Exact(3),
            Op::Cond => Arity::Exact(3),
            Op::ToUpper
            | Op::ToLower
            | Op::StrLenCp
            | Op::Not
            | Op::Year
            | Op::Month
            | Op::DayOfMonth
            | Op::Hour
            | Op::Minute
            | Op::Second
            | Op::Literal => Arity::Exact(1),
        }
    }

    /// Whether the operator wants its arguments evaluated before it runs.
    ///
    /// `$cond` and `$ifNull` are the exceptions and must not be: evaluating
    /// every branch would make `{$cond: [{$gt: ["$n", 0]}, {$divide: [1,
    /// "$n"]}, 0]}` fail on exactly the inputs the guard exists to protect.
    /// `$literal` does not evaluate its argument at all.
    fn is_lazy(self) -> bool {
        matches!(self, Op::Cond | Op::IfNull | Op::Literal)
    }
}

// ---------------------------------------------------------------------------
// The tree
// ---------------------------------------------------------------------------

/// A computed expression.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// `"$qty"` — the value at a dot path in the incoming document.
    Field(String),
    /// Any other BSON value, used as-is.
    Literal(Bson),
    /// An operator over positional arguments.
    Op(Op, Vec<Expr>),
    /// `{$switch: {branches: [{case, then}, ...], default: <expr>}}`.
    Switch { branches: Vec<(Expr, Expr)>, default: Option<Box<Expr>> },
    /// `{$dateToString: {date: <expr>, format: "<fmt>"}}`.
    DateToString { date: Box<Expr>, format: String },
    /// A document whose values are expressions.
    Object(Vec<(String, Expr)>),
}

impl Expr {
    /// Parse an expression from BSON.
    pub fn parse(value: &Bson) -> Result<Self> {
        match value {
            Bson::String(s) => Ok(match s.strip_prefix('$') {
                // `$$ROOT` and friends are not supported: they need an
                // evaluation scope, which is a deliberate M9 exclusion.
                Some(field) if field.starts_with('$') => {
                    return Err(Error::UnsupportedOperator(format!(
                        "variable expressions like {s:?} are not supported"
                    )));
                }
                Some(field) if !field.is_empty() => Expr::Field(field.to_string()),
                // A bare string without `$` is a literal, which is what
                // MongoDB does: `{$sum: "total"}` sums the constant.
                _ => Expr::Literal(value.clone()),
            }),
            Bson::Document(doc) => Self::parse_document(doc),
            other => Ok(Expr::Literal(other.clone())),
        }
    }

    fn parse_document(doc: &Document) -> Result<Self> {
        let Some((first, _)) = doc.iter().next() else {
            // `{}` is an empty object expression, not an operator.
            return Ok(Expr::Object(Vec::new()));
        };

        if !first.starts_with('$') {
            // An object expression. Every key must be a plain name.
            let mut fields = Vec::with_capacity(doc.len());
            for (key, value) in doc {
                if key.starts_with('$') {
                    return Err(Error::InvalidQuery(format!(
                        "cannot mix operator {key:?} with field {first:?} in one expression"
                    )));
                }
                fields.push((key.clone(), Self::parse(value)?));
            }
            return Ok(Expr::Object(fields));
        }

        if doc.len() > 1 {
            return Err(Error::InvalidQuery(format!(
                "an operator expression takes exactly one key, found {} alongside {first:?}",
                doc.len() - 1
            )));
        }

        let raw = doc.get(first).expect("key from the same document");
        match first.as_str() {
            "$switch" => Self::parse_switch(raw),
            "$dateToString" => Self::parse_date_to_string(raw),
            name => {
                let Some(op) = Op::from_name(name) else {
                    return Err(Error::UnsupportedOperator(format!(
                        "unknown expression operator {name:?}"
                    )));
                };
                if op == Op::Literal {
                    return Ok(Expr::Op(op, vec![Expr::Literal(raw.clone())]));
                }
                let args = Self::parse_args(op, raw)?;
                Ok(Expr::Op(op, args))
            }
        }
    }

    /// Arguments are an array, or a single value when the operator takes one.
    ///
    /// MongoDB allows `{$toUpper: "$name"}` as well as `{$toUpper: ["$name"]}`,
    /// and the shorthand is what people actually write.
    fn parse_args(op: Op, raw: &Bson) -> Result<Vec<Expr>> {
        let args = match raw {
            Bson::Array(items) => items.iter().map(Self::parse).collect::<Result<Vec<_>>>()?,
            single => vec![Self::parse(single)?],
        };

        let ok = match op.arity() {
            Arity::Exact(n) => args.len() == n,
            Arity::AtLeast(n) => args.len() >= n,
        };
        if !ok {
            let want = match op.arity() {
                Arity::Exact(n) => format!("exactly {n}"),
                Arity::AtLeast(n) => format!("at least {n}"),
            };
            return Err(Error::InvalidQuery(format!(
                "{} takes {want} argument(s), found {}",
                op.name(),
                args.len()
            )));
        }
        Ok(args)
    }

    fn parse_switch(raw: &Bson) -> Result<Self> {
        let Bson::Document(spec) = raw else {
            return Err(Error::InvalidQuery(format!(
                "$switch takes a document, found {}",
                type_name(raw)
            )));
        };
        let Some(Bson::Array(raw_branches)) = spec.get("branches") else {
            return Err(Error::InvalidQuery("$switch needs a `branches` array".into()));
        };
        if raw_branches.is_empty() {
            return Err(Error::InvalidQuery("$switch needs at least one branch".into()));
        }

        let mut branches = Vec::with_capacity(raw_branches.len());
        for branch in raw_branches {
            let Bson::Document(b) = branch else {
                return Err(Error::InvalidQuery(format!(
                    "each $switch branch is a document, found {}",
                    type_name(branch)
                )));
            };
            let (Some(case), Some(then)) = (b.get("case"), b.get("then")) else {
                return Err(Error::InvalidQuery(
                    "each $switch branch needs `case` and `then`".into(),
                ));
            };
            branches.push((Self::parse(case)?, Self::parse(then)?));
        }

        let default = match spec.get("default") {
            Some(d) => Some(Box::new(Self::parse(d)?)),
            None => None,
        };
        Ok(Expr::Switch { branches, default })
    }

    fn parse_date_to_string(raw: &Bson) -> Result<Self> {
        let Bson::Document(spec) = raw else {
            return Err(Error::InvalidQuery(format!(
                "$dateToString takes a document, found {}",
                type_name(raw)
            )));
        };
        let Some(date) = spec.get("date") else {
            return Err(Error::InvalidQuery("$dateToString needs a `date`".into()));
        };
        // The format is a constant rather than an expression. Making it
        // computed would mean re-parsing the specifier string per document for
        // a flexibility nobody has asked for.
        let format = match spec.get("format") {
            Some(Bson::String(f)) => f.clone(),
            None => "%Y-%m-%dT%H:%M:%S.%LZ".to_string(),
            Some(other) => {
                return Err(Error::InvalidQuery(format!(
                    "$dateToString `format` is a string, found {}",
                    type_name(other)
                )));
            }
        };
        Ok(Expr::DateToString { date: Box::new(Self::parse(date)?), format })
    }

    /// Resolve against a document.
    ///
    /// A missing field is `Null`, matching how the filter layer treats absence.
    pub fn eval(&self, doc: &Document) -> Result<Bson> {
        match self {
            Expr::Field(p) => {
                Ok(path::resolve(doc, p).into_iter().next().cloned().unwrap_or(Bson::Null))
            }
            Expr::Literal(v) => Ok(v.clone()),
            Expr::Object(fields) => {
                let mut out = Document::new();
                for (key, expr) in fields {
                    out.insert(key.clone(), expr.eval(doc)?);
                }
                Ok(Bson::Document(out))
            }
            Expr::Switch { branches, default } => {
                for (case, then) in branches {
                    if truthy(&case.eval(doc)?) {
                        return then.eval(doc);
                    }
                }
                match default {
                    Some(d) => d.eval(doc),
                    None => Err(Error::InvalidQuery(
                        "no $switch branch matched and there is no `default`".into(),
                    )),
                }
            }
            Expr::DateToString { date, format } => match date.eval(doc)? {
                Bson::Null => Ok(Bson::Null),
                Bson::DateTime(dt) => Ok(Bson::String(format_date(dt.timestamp_millis(), format)?)),
                other => Err(Error::InvalidQuery(format!(
                    "$dateToString needs a date, found {}",
                    type_name(&other)
                ))),
            },
            Expr::Op(op, args) if op.is_lazy() => eval_lazy(*op, args, doc),
            Expr::Op(op, args) => {
                let values = args.iter().map(|a| a.eval(doc)).collect::<Result<Vec<_>>>()?;
                eval_op(*op, &values)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// The operators that must not have their arguments pre-evaluated.
fn eval_lazy(op: Op, args: &[Expr], doc: &Document) -> Result<Bson> {
    match op {
        Op::Literal => match args {
            [inner] => inner.eval(doc),
            _ => unreachable!("arity checked at parse"),
        },
        Op::Cond => match args {
            [cond, then, otherwise] => {
                if truthy(&cond.eval(doc)?) {
                    then.eval(doc)
                } else {
                    otherwise.eval(doc)
                }
            }
            _ => unreachable!("arity checked at parse"),
        },
        Op::IfNull => match args {
            [value, fallback] => {
                let v = value.eval(doc)?;
                if matches!(v, Bson::Null | Bson::Undefined) { fallback.eval(doc) } else { Ok(v) }
            }
            _ => unreachable!("arity checked at parse"),
        },
        _ => unreachable!("only lazy operators reach here"),
    }
}

fn eval_op(op: Op, args: &[Bson]) -> Result<Bson> {
    match op {
        Op::Add => arithmetic_add(args),
        Op::Subtract => arithmetic_subtract(&args[0], &args[1]),
        Op::Multiply => fold_numeric(op, args, Num::mul),
        Op::Divide => divide(&args[0], &args[1]),
        Op::Mod => modulo(&args[0], &args[1]),

        Op::Concat => concat(args),
        Op::ToUpper => Ok(Bson::String(as_string_lossy(&args[0], op)?.to_uppercase())),
        Op::ToLower => Ok(Bson::String(as_string_lossy(&args[0], op)?.to_lowercase())),
        Op::Substr => substr(args),
        Op::Split => split(&args[0], &args[1]),
        Op::StrLenCp => Ok(Bson::Int64(as_string_strict(&args[0], op)?.chars().count() as i64)),

        Op::Eq => Ok(Bson::Boolean(canonical_cmp(&args[0], &args[1]) == Ordering::Equal)),
        Op::Ne => Ok(Bson::Boolean(canonical_cmp(&args[0], &args[1]) != Ordering::Equal)),
        Op::Gt => Ok(Bson::Boolean(canonical_cmp(&args[0], &args[1]) == Ordering::Greater)),
        Op::Gte => Ok(Bson::Boolean(canonical_cmp(&args[0], &args[1]) != Ordering::Less)),
        Op::Lt => Ok(Bson::Boolean(canonical_cmp(&args[0], &args[1]) == Ordering::Less)),
        Op::Lte => Ok(Bson::Boolean(canonical_cmp(&args[0], &args[1]) != Ordering::Greater)),
        Op::Cmp => Ok(Bson::Int32(match canonical_cmp(&args[0], &args[1]) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        })),

        Op::And => Ok(Bson::Boolean(args.iter().all(truthy))),
        Op::Or => Ok(Bson::Boolean(args.iter().any(truthy))),
        Op::Not => Ok(Bson::Boolean(!truthy(&args[0]))),

        Op::Year | Op::Month | Op::DayOfMonth | Op::Hour | Op::Minute | Op::Second => {
            date_part(op, &args[0])
        }

        Op::Cond | Op::IfNull | Op::Literal => unreachable!("handled lazily"),
    }
}

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

/// A number that remembers whether it is still exact.
#[derive(Clone, Copy, Debug)]
enum Num {
    Int(i64),
    Dbl(f64),
}

impl Num {
    fn from_bson(value: &Bson) -> Option<Self> {
        match value {
            Bson::Int32(n) => Some(Num::Int(i64::from(*n))),
            Bson::Int64(n) => Some(Num::Int(*n)),
            Bson::Double(d) => Some(Num::Dbl(*d)),
            _ => None,
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Num::Int(n) => n as f64,
            Num::Dbl(d) => d,
        }
    }

    fn to_bson(self) -> Bson {
        match self {
            Num::Int(n) => Bson::Int64(n),
            Num::Dbl(d) => Bson::Double(d),
        }
    }

    /// Integer arithmetic stays integral until it cannot.
    ///
    /// The overflow arm is why this is `checked_*` rather than a cast: falling
    /// to `f64` loses precision, but wrapping would produce a number of the
    /// wrong sign, which is worse than approximate.
    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Num::Int(a), Num::Int(b)) => match a.checked_add(b) {
                Some(n) => Num::Int(n),
                None => Num::Dbl(a as f64 + b as f64),
            },
            _ => Num::Dbl(self.as_f64() + other.as_f64()),
        }
    }

    fn sub(self, other: Self) -> Self {
        match (self, other) {
            (Num::Int(a), Num::Int(b)) => match a.checked_sub(b) {
                Some(n) => Num::Int(n),
                None => Num::Dbl(a as f64 - b as f64),
            },
            _ => Num::Dbl(self.as_f64() - other.as_f64()),
        }
    }

    fn mul(self, other: Self) -> Self {
        match (self, other) {
            (Num::Int(a), Num::Int(b)) => match a.checked_mul(b) {
                Some(n) => Num::Int(n),
                None => Num::Dbl(a as f64 * b as f64),
            },
            _ => Num::Dbl(self.as_f64() * other.as_f64()),
        }
    }
}

/// Accumulate integers exactly, and only widen when something forces it.
///
/// Exposed to `aggregate` so `$sum` shares this path rather than keeping its
/// own `f64`-with-a-flag version, which lost precision above 2^53 despite a
/// comment saying it must not.
#[derive(Clone, Copy, Debug, Default)]
pub struct Total(Option<Num>);

impl Total {
    pub fn add(&mut self, value: &Bson) {
        // A non-numeric operand is ignored, which is what `$sum` has always
        // done: summing a field that is sometimes a string should total the
        // numbers, not refuse the whole group.
        let Some(n) = Num::from_bson(value) else {
            return;
        };
        self.0 = Some(match self.0 {
            Some(acc) => acc.add(n),
            None => n,
        });
    }

    pub fn to_bson(self) -> Bson {
        self.0.unwrap_or(Num::Int(0)).to_bson()
    }

    pub fn as_f64(self) -> f64 {
        self.0.map_or(0.0, Num::as_f64)
    }
}

fn numbers(op: Op, args: &[Bson]) -> Result<Option<Vec<Num>>> {
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            // Null propagates through arithmetic rather than erroring.
            Bson::Null | Bson::Undefined => return Ok(None),
            other => match Num::from_bson(other) {
                Some(n) => out.push(n),
                None => {
                    return Err(Error::InvalidQuery(format!(
                        "{} needs numbers, found {}",
                        op.name(),
                        type_name(other)
                    )));
                }
            },
        }
    }
    Ok(Some(out))
}

fn fold_numeric(op: Op, args: &[Bson], f: fn(Num, Num) -> Num) -> Result<Bson> {
    let Some(nums) = numbers(op, args)? else {
        return Ok(Bson::Null);
    };
    let mut it = nums.into_iter();
    let first = it.next().expect("arity checked at parse");
    Ok(it.fold(first, f).to_bson())
}

/// `$add` is arithmetic *or* date shifting, decided by its operands.
///
/// At most one date may appear: adding two dates is meaningless, and MongoDB
/// refuses it rather than treating one as a millisecond count.
fn arithmetic_add(args: &[Bson]) -> Result<Bson> {
    let dates = args.iter().filter(|a| matches!(a, Bson::DateTime(_))).count();
    if dates == 0 {
        return fold_numeric(Op::Add, args, Num::add);
    }
    if dates > 1 {
        return Err(Error::InvalidQuery(
            "$add takes at most one date; adding two dates has no meaning".into(),
        ));
    }

    let mut millis = 0i64;
    let mut base = 0i64;
    for arg in args {
        match arg {
            Bson::DateTime(dt) => base = dt.timestamp_millis(),
            Bson::Null | Bson::Undefined => return Ok(Bson::Null),
            other => match Num::from_bson(other) {
                Some(n) => millis = millis.saturating_add(n.as_f64() as i64),
                None => {
                    return Err(Error::InvalidQuery(format!(
                        "$add needs numbers or one date, found {}",
                        type_name(other)
                    )));
                }
            },
        }
    }
    Ok(Bson::DateTime(bson::DateTime::from_millis(base.saturating_add(millis))))
}

/// `$subtract` on two dates is an interval in milliseconds; on a date and a
/// number it shifts the date. Both are MongoDB's behaviour and both are what
/// makes date arithmetic usable at all.
fn arithmetic_subtract(a: &Bson, b: &Bson) -> Result<Bson> {
    match (a, b) {
        (Bson::Null | Bson::Undefined, _) | (_, Bson::Null | Bson::Undefined) => Ok(Bson::Null),
        (Bson::DateTime(x), Bson::DateTime(y)) => {
            Ok(Bson::Int64(x.timestamp_millis().saturating_sub(y.timestamp_millis())))
        }
        (Bson::DateTime(x), other) => match Num::from_bson(other) {
            Some(n) => Ok(Bson::DateTime(bson::DateTime::from_millis(
                x.timestamp_millis().saturating_sub(n.as_f64() as i64),
            ))),
            None => Err(Error::InvalidQuery(format!(
                "$subtract needs a date or a number, found {}",
                type_name(other)
            ))),
        },
        (_, Bson::DateTime(_)) => {
            Err(Error::InvalidQuery("$subtract cannot take a date away from a number".into()))
        }
        _ => fold_numeric(Op::Subtract, &[a.clone(), b.clone()], Num::sub),
    }
}

fn divide(a: &Bson, b: &Bson) -> Result<Bson> {
    let Some(nums) = numbers(Op::Divide, &[a.clone(), b.clone()])? else {
        return Ok(Bson::Null);
    };
    let divisor = nums[1].as_f64();
    if divisor == 0.0 {
        return Err(Error::InvalidQuery("$divide by zero".into()));
    }
    // Always a double, as MongoDB does: an integer result would make
    // `{$divide: [1, 2]}` round to zero, which is a wrong answer rather than
    // an imprecise one.
    Ok(Bson::Double(nums[0].as_f64() / divisor))
}

fn modulo(a: &Bson, b: &Bson) -> Result<Bson> {
    let Some(nums) = numbers(Op::Mod, &[a.clone(), b.clone()])? else {
        return Ok(Bson::Null);
    };
    match (nums[0], nums[1]) {
        (_, Num::Int(0)) => Err(Error::InvalidQuery("$mod by zero".into())),
        (Num::Int(x), Num::Int(y)) => Ok(Bson::Int64(x % y)),
        (x, y) if y.as_f64() == 0.0 => {
            let _ = x;
            Err(Error::InvalidQuery("$mod by zero".into()))
        }
        (x, y) => Ok(Bson::Double(x.as_f64() % y.as_f64())),
    }
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// `$toUpper` and `$toLower` treat absence as the empty string, which is what
/// MongoDB does and what makes them safe over sparse documents.
fn as_string_lossy(value: &Bson, op: Op) -> Result<String> {
    match value {
        Bson::String(s) => Ok(s.clone()),
        Bson::Null | Bson::Undefined => Ok(String::new()),
        other => Err(Error::InvalidQuery(format!(
            "{} needs a string, found {}",
            op.name(),
            type_name(other)
        ))),
    }
}

fn as_string_strict(value: &Bson, op: Op) -> Result<String> {
    match value {
        Bson::String(s) => Ok(s.clone()),
        other => Err(Error::InvalidQuery(format!(
            "{} needs a string, found {}",
            op.name(),
            type_name(other)
        ))),
    }
}

fn concat(args: &[Bson]) -> Result<Bson> {
    let mut out = String::new();
    for arg in args {
        match arg {
            // One null makes the whole concatenation null, so a missing field
            // cannot silently vanish from the middle of a joined string.
            Bson::Null | Bson::Undefined => return Ok(Bson::Null),
            Bson::String(s) => out.push_str(s),
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$concat needs strings, found {}",
                    type_name(other)
                )));
            }
        }
    }
    Ok(Bson::String(out))
}

/// Counted in **code points**, not bytes.
///
/// The byte-oriented `$substrBytes` can split a multi-byte character and
/// produce invalid UTF-8; there is no reason to offer that here, so `$substr`
/// and `$substrCP` are the same operator.
fn substr(args: &[Bson]) -> Result<Bson> {
    let s = as_string_lossy(&args[0], Op::Substr)?;
    let start = as_index(&args[1], "$substr start")?;
    let len = match &args[2] {
        // MongoDB's convention: a negative length means "to the end".
        Bson::Int32(n) if *n < 0 => None,
        Bson::Int64(n) if *n < 0 => None,
        Bson::Double(d) if *d < 0.0 => None,
        other => Some(as_index(other, "$substr length")?),
    };

    let taken: String = match len {
        Some(n) => s.chars().skip(start).take(n).collect(),
        None => s.chars().skip(start).collect(),
    };
    Ok(Bson::String(taken))
}

fn as_index(value: &Bson, what: &str) -> Result<usize> {
    let n = match value {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        Bson::Double(d) => *d as i64,
        other => {
            return Err(Error::InvalidQuery(format!(
                "{what} must be a number, found {}",
                type_name(other)
            )));
        }
    };
    usize::try_from(n)
        .map_err(|_| Error::InvalidQuery(format!("{what} cannot be negative, found {n}")))
}

fn split(value: &Bson, delimiter: &Bson) -> Result<Bson> {
    if matches!(value, Bson::Null | Bson::Undefined) {
        return Ok(Bson::Null);
    }
    let s = as_string_strict(value, Op::Split)?;
    let d = as_string_strict(delimiter, Op::Split)?;
    if d.is_empty() {
        // Splitting on "" would yield one element per character plus two empty
        // ends, which is never what a caller means and is a silent surprise.
        return Err(Error::InvalidQuery("$split needs a non-empty delimiter".into()));
    }
    Ok(Bson::Array(s.split(d.as_str()).map(|part| Bson::String(part.to_string())).collect()))
}

// ---------------------------------------------------------------------------
// Dates
// ---------------------------------------------------------------------------

fn date_part(op: Op, value: &Bson) -> Result<Bson> {
    let millis = match value {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::DateTime(dt) => dt.timestamp_millis(),
        other => {
            return Err(Error::InvalidQuery(format!(
                "{} needs a date, found {}",
                op.name(),
                type_name(other)
            )));
        }
    };
    let parts = Civil::from_millis(millis);
    Ok(Bson::Int32(match op {
        Op::Year => parts.year,
        Op::Month => parts.month,
        Op::DayOfMonth => parts.day,
        Op::Hour => parts.hour,
        Op::Minute => parts.minute,
        Op::Second => parts.second,
        _ => unreachable!("only date operators reach here"),
    }))
}

/// A UTC calendar date broken into parts.
///
/// Computed here rather than through `chrono` because the conversion is
/// twenty lines of well-known arithmetic and the alternative is a new runtime
/// dependency for it. Everything is UTC: BSON dates carry no zone, so there is
/// nothing to convert to and offering `%z` would be inventing information.
#[derive(Debug, PartialEq, Eq)]
struct Civil {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    milli: i32,
}

impl Civil {
    fn from_millis(millis: i64) -> Self {
        // Floor division, so dates before 1970 do not round towards zero and
        // land a day late.
        let days = millis.div_euclid(86_400_000);
        let rem = millis.rem_euclid(86_400_000);

        // Howard Hinnant's civil_from_days, shifted to a 1 March year start so
        // the leap day falls at the end of the cycle.
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if month <= 2 { y + 1 } else { y };

        Self {
            year: year as i32,
            month: month as i32,
            day: day as i32,
            hour: (rem / 3_600_000) as i32,
            minute: (rem / 60_000 % 60) as i32,
            second: (rem / 1_000 % 60) as i32,
            milli: (rem % 1_000) as i32,
        }
    }
}

/// The `$dateToString` specifier subset.
///
/// `%Y %m %d %H %M %S %L %%` — the parts a date can actually be broken into
/// here. A specifier this does not know is an **error** rather than being
/// copied through: a silently literal `%q` in every row is the kind of wrong
/// output nobody notices until it is in a report.
fn format_date(millis: i64, format: &str) -> Result<String> {
    let c = Civil::from_millis(millis);
    let mut out = String::with_capacity(format.len() + 8);
    let mut chars = format.chars();

    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('Y') => out.push_str(&format!("{:04}", c.year)),
            Some('m') => out.push_str(&format!("{:02}", c.month)),
            Some('d') => out.push_str(&format!("{:02}", c.day)),
            Some('H') => out.push_str(&format!("{:02}", c.hour)),
            Some('M') => out.push_str(&format!("{:02}", c.minute)),
            Some('S') => out.push_str(&format!("{:02}", c.second)),
            Some('L') => out.push_str(&format!("{:03}", c.milli)),
            Some('%') => out.push('%'),
            Some(other) => {
                return Err(Error::UnsupportedOperator(format!(
                    "$dateToString does not support the specifier %{other}"
                )));
            }
            None => {
                return Err(Error::InvalidQuery("$dateToString format ends with a bare %".into()));
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// MongoDB's truthiness: `false`, `null`, missing and zero are false, and
/// **everything else** — including the empty string and the empty array — is
/// true.
pub fn truthy(value: &Bson) -> bool {
    match value {
        Bson::Boolean(b) => *b,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(0) | Bson::Int64(0) => false,
        Bson::Double(d) => *d != 0.0,
        _ => true,
    }
}

pub(crate) fn type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "a double",
        Bson::String(_) => "a string",
        Bson::Array(_) => "an array",
        Bson::Document(_) => "a document",
        Bson::Boolean(_) => "a boolean",
        Bson::Null => "null",
        Bson::Int32(_) | Bson::Int64(_) => "an integer",
        Bson::DateTime(_) => "a date",
        _ => "that type",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    /// Evaluate an expression written the way a caller writes it.
    fn ev(expr: Bson, doc: &Document) -> Result<Bson> {
        Expr::parse(&expr)?.eval(doc)
    }

    fn ok(expr: Bson) -> Bson {
        ev(expr, &Document::new()).expect("expression should evaluate")
    }

    fn on(expr: Bson, d: Document) -> Bson {
        ev(expr, &d).expect("expression should evaluate")
    }

    // -- parsing ----------------------------------------------------------

    #[test]
    fn a_dollar_string_is_a_field_and_a_bare_one_is_a_literal() {
        assert_eq!(on(Bson::String("$n".into()), doc! {"n": 7}), Bson::Int32(7));
        assert_eq!(on(Bson::String("n".into()), doc! {"n": 7}), Bson::String("n".into()));
    }

    #[test]
    fn a_missing_field_is_null() {
        assert_eq!(on(Bson::String("$nope".into()), doc! {"n": 7}), Bson::Null);
    }

    #[test]
    fn an_unknown_operator_is_refused() {
        assert!(ev(doc! {"$frobnicate": [1]}.into(), &Document::new()).is_err());
    }

    #[test]
    fn arity_is_checked_at_parse() {
        assert!(Expr::parse(&doc! {"$subtract": [1]}.into()).is_err());
        assert!(Expr::parse(&doc! {"$subtract": [1, 2, 3]}.into()).is_err());
        assert!(Expr::parse(&doc! {"$subtract": [1, 2]}.into()).is_ok());
    }

    #[test]
    fn a_single_argument_operator_takes_the_shorthand() {
        // `{$toUpper: "$name"}` as well as `{$toUpper: ["$name"]}`.
        assert_eq!(
            on(doc! {"$toUpper": "$name"}.into(), doc! {"name": "ada"}),
            Bson::String("ADA".into())
        );
    }

    #[test]
    fn an_operator_cannot_share_a_document_with_a_field() {
        assert!(Expr::parse(&doc! {"$add": [1, 2], "other": 1}.into()).is_err());
        assert!(Expr::parse(&doc! {"other": 1, "$add": [1, 2]}.into()).is_err());
    }

    #[test]
    fn variable_expressions_are_refused_rather_than_read_as_a_field() {
        // `$$ROOT` needs an evaluation scope, which is deliberately out of
        // scope. Parsing it as a field named `$ROOT` would silently yield null.
        assert!(Expr::parse(&Bson::String("$$ROOT".into())).is_err());
    }

    // -- object expressions, the behaviour change -------------------------

    #[test]
    fn a_plain_document_computes_its_values() {
        assert_eq!(
            on(doc! {"a": "$x", "b": 2}.into(), doc! {"x": 41}),
            Bson::Document(doc! {"a": 41, "b": 2})
        );
    }

    #[test]
    fn literal_escapes_the_field_and_document_conventions() {
        assert_eq!(ok(doc! {"$literal": "$notAField"}.into()), Bson::String("$notAField".into()));
        assert_eq!(ok(doc! {"$literal": {"a": "$x"}}.into()), Bson::Document(doc! {"a": "$x"}));
    }

    // -- arithmetic -------------------------------------------------------

    #[test]
    fn integer_arithmetic_stays_integral() {
        assert_eq!(ok(doc! {"$add": [2, 3]}.into()), Bson::Int64(5));
        assert_eq!(ok(doc! {"$subtract": [10, 3]}.into()), Bson::Int64(7));
        assert_eq!(ok(doc! {"$multiply": [6, 7]}.into()), Bson::Int64(42));
    }

    #[test]
    fn integer_arithmetic_is_exact_above_two_to_the_fifty_three() {
        // The whole point of the i64 path. Accumulating in f64 gives
        // 9007199254740992 here, because 2^53 + 1 is not representable.
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(ok(doc! {"$add": [big, 1i64]}.into()), Bson::Int64(big + 1));
    }

    #[test]
    fn overflow_promotes_to_double_rather_than_wrapping() {
        // Wrapping would flip the sign, which is worse than approximate.
        let out = ok(doc! {"$add": [i64::MAX, 1i64]}.into());
        match out {
            Bson::Double(d) => assert!(d > 9.0e18),
            other => panic!("expected a double, got {other:?}"),
        }
    }

    #[test]
    fn a_double_operand_makes_the_result_a_double() {
        assert_eq!(ok(doc! {"$add": [2, 0.5]}.into()), Bson::Double(2.5));
    }

    #[test]
    fn divide_is_always_a_double() {
        // Integer division would make {$divide: [1, 2]} zero, which is a wrong
        // answer rather than an imprecise one.
        assert_eq!(ok(doc! {"$divide": [1, 2]}.into()), Bson::Double(0.5));
        assert_eq!(ok(doc! {"$divide": [4, 2]}.into()), Bson::Double(2.0));
    }

    #[test]
    fn dividing_or_modding_by_zero_is_an_error() {
        assert!(ev(doc! {"$divide": [1, 0]}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$mod": [1, 0]}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$divide": [1, 0.0]}.into(), &Document::new()).is_err());
    }

    #[test]
    fn mod_keeps_integers_integral() {
        assert_eq!(ok(doc! {"$mod": [7, 3]}.into()), Bson::Int64(1));
    }

    #[test]
    fn null_propagates_through_arithmetic() {
        assert_eq!(on(doc! {"$add": ["$missing", 1]}.into(), doc! {}), Bson::Null);
        assert_eq!(on(doc! {"$multiply": ["$missing", 2]}.into(), doc! {}), Bson::Null);
    }

    #[test]
    fn a_non_numeric_operand_is_an_error_not_a_null() {
        // The distinction that matters: a typo yields null, a type error
        // refuses. Collapsing both to null makes them indistinguishable.
        assert!(ev(doc! {"$add": ["text", 1]}.into(), &Document::new()).is_err());
    }

    #[test]
    fn variadic_operators_take_more_than_two_arguments() {
        assert_eq!(ok(doc! {"$add": [1, 2, 3, 4]}.into()), Bson::Int64(10));
        assert_eq!(ok(doc! {"$multiply": [2, 3, 4]}.into()), Bson::Int64(24));
    }

    // -- date arithmetic --------------------------------------------------

    fn dt(millis: i64) -> Bson {
        Bson::DateTime(bson::DateTime::from_millis(millis))
    }

    #[test]
    fn adding_a_number_to_a_date_shifts_it() {
        assert_eq!(ok(doc! {"$add": [dt(1_000), 500i64]}.into()), dt(1_500));
    }

    #[test]
    fn subtracting_two_dates_gives_milliseconds() {
        assert_eq!(ok(doc! {"$subtract": [dt(5_000), dt(1_500)]}.into()), Bson::Int64(3_500));
    }

    #[test]
    fn subtracting_a_number_from_a_date_shifts_it_back() {
        assert_eq!(ok(doc! {"$subtract": [dt(5_000), 500i64]}.into()), dt(4_500));
    }

    #[test]
    fn adding_two_dates_is_refused() {
        assert!(ev(doc! {"$add": [dt(1), dt(2)]}.into(), &Document::new()).is_err());
    }

    #[test]
    fn taking_a_date_away_from_a_number_is_refused() {
        assert!(ev(doc! {"$subtract": [5, dt(1)]}.into(), &Document::new()).is_err());
    }

    // -- strings ----------------------------------------------------------

    #[test]
    fn concat_joins_and_null_poisons() {
        assert_eq!(
            on(doc! {"$concat": ["$a", " ", "$b"]}.into(), doc! {"a": "grace", "b": "hopper"}),
            Bson::String("grace hopper".into())
        );
        // A missing middle must not silently vanish from the joined string.
        assert_eq!(on(doc! {"$concat": ["a", "$gone", "b"]}.into(), doc! {}), Bson::Null);
    }

    #[test]
    fn casing_treats_absence_as_empty_but_refuses_a_number() {
        assert_eq!(on(doc! {"$toUpper": "$gone"}.into(), doc! {}), Bson::String(String::new()));
        assert_eq!(
            on(doc! {"$toLower": "$s"}.into(), doc! {"s": "ABC"}),
            Bson::String("abc".into())
        );
        assert!(ev(doc! {"$toUpper": 5}.into(), &Document::new()).is_err());
    }

    #[test]
    fn substr_counts_code_points_not_bytes() {
        // A byte offset here would split the multi-byte character and produce
        // something that is not valid UTF-8.
        assert_eq!(
            on(doc! {"$substr": ["$s", 0, 3]}.into(), doc! {"s": "héllo"}),
            Bson::String("hél".into())
        );
        assert_eq!(
            on(doc! {"$substr": ["$s", 1, 2]}.into(), doc! {"s": "héllo"}),
            Bson::String("él".into())
        );
    }

    #[test]
    fn a_negative_substr_length_means_to_the_end() {
        assert_eq!(
            on(doc! {"$substr": ["$s", 2, -1]}.into(), doc! {"s": "abcdef"}),
            Bson::String("cdef".into())
        );
    }

    #[test]
    fn substr_past_the_end_is_empty_rather_than_an_error() {
        assert_eq!(
            on(doc! {"$substr": ["$s", 99, 3]}.into(), doc! {"s": "abc"}),
            Bson::String(String::new())
        );
    }

    #[test]
    fn split_needs_a_non_empty_delimiter() {
        assert_eq!(
            on(doc! {"$split": ["$s", ","]}.into(), doc! {"s": "a,b,c"}),
            Bson::Array(vec!["a".into(), "b".into(), "c".into()])
        );
        assert!(ev(doc! {"$split": ["abc", ""]}.into(), &Document::new()).is_err());
    }

    #[test]
    fn strlen_counts_code_points() {
        assert_eq!(on(doc! {"$strLenCP": "$s"}.into(), doc! {"s": "héllo"}), Bson::Int64(5));
    }

    // -- conditionals -----------------------------------------------------

    #[test]
    fn cond_picks_a_branch() {
        assert_eq!(ok(doc! {"$cond": [true, "yes", "no"]}.into()), Bson::String("yes".into()));
        assert_eq!(ok(doc! {"$cond": [false, "yes", "no"]}.into()), Bson::String("no".into()));
    }

    #[test]
    fn cond_does_not_evaluate_the_branch_it_did_not_take() {
        // The guard exists precisely to protect the divide; evaluating both
        // branches eagerly would fail on exactly the inputs it guards.
        let expr = doc! {
            "$cond": [{"$gt": ["$n", 0]}, {"$divide": [100, "$n"]}, Bson::Null]
        };
        assert_eq!(on(expr.clone().into(), doc! {"n": 0}), Bson::Null);
        assert_eq!(on(expr.into(), doc! {"n": 4}), Bson::Double(25.0));
    }

    #[test]
    fn ifnull_falls_back_and_is_also_lazy() {
        assert_eq!(
            on(doc! {"$ifNull": ["$gone", "dflt"]}.into(), doc! {}),
            Bson::String("dflt".into())
        );
        assert_eq!(on(doc! {"$ifNull": ["$n", "dflt"]}.into(), doc! {"n": 3}), Bson::Int32(3));
        // The fallback is not evaluated when it is not needed.
        assert_eq!(
            on(doc! {"$ifNull": ["$n", {"$divide": [1, 0]}]}.into(), doc! {"n": 3}),
            Bson::Int32(3)
        );
    }

    #[test]
    fn switch_takes_the_first_matching_branch() {
        let expr = doc! {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$n", 10]}, "then": "small"},
                    {"case": {"$lt": ["$n", 100]}, "then": "medium"},
                ],
                "default": "large",
            }
        };
        assert_eq!(on(expr.clone().into(), doc! {"n": 5}), Bson::String("small".into()));
        assert_eq!(on(expr.clone().into(), doc! {"n": 50}), Bson::String("medium".into()));
        assert_eq!(on(expr.into(), doc! {"n": 500}), Bson::String("large".into()));
    }

    #[test]
    fn switch_without_a_default_and_no_match_is_an_error() {
        let expr = doc! {
            "$switch": {"branches": [{"case": false, "then": "never"}]}
        };
        assert!(ev(expr.into(), &Document::new()).is_err());
    }

    #[test]
    fn switch_needs_at_least_one_well_formed_branch() {
        assert!(Expr::parse(&doc! {"$switch": {"branches": []}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$switch": {"branches": [{"case": true}]}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$switch": {"default": 1}}.into()).is_err());
    }

    // -- comparison and boolean -------------------------------------------

    #[test]
    fn comparison_returns_booleans() {
        assert_eq!(ok(doc! {"$eq": [1, 1]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$ne": [1, 2]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$gt": [2, 1]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$gte": [1, 1]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$lt": [1, 2]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$lte": [1, 1]}.into()), Bson::Boolean(true));
    }

    #[test]
    fn comparison_uses_the_canonical_cross_type_order() {
        // 5 and 5.0 are equal, exactly as they are to an index.
        assert_eq!(ok(doc! {"$eq": [5, 5.0]}.into()), Bson::Boolean(true));
        // And a number sorts below a string rather than erroring.
        assert_eq!(ok(doc! {"$lt": [5, "a"]}.into()), Bson::Boolean(true));
    }

    #[test]
    fn cmp_returns_minus_one_zero_or_one() {
        assert_eq!(ok(doc! {"$cmp": [1, 2]}.into()), Bson::Int32(-1));
        assert_eq!(ok(doc! {"$cmp": [2, 2]}.into()), Bson::Int32(0));
        assert_eq!(ok(doc! {"$cmp": [3, 2]}.into()), Bson::Int32(1));
    }

    #[test]
    fn boolean_operators_follow_mongo_truthiness() {
        assert_eq!(ok(doc! {"$and": [1, "text", true]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$and": [1, 0]}.into()), Bson::Boolean(false));
        assert_eq!(ok(doc! {"$or": [0, Bson::Null, 1]}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$or": [0, Bson::Null]}.into()), Bson::Boolean(false));
        assert_eq!(ok(doc! {"$not": [0]}.into()), Bson::Boolean(true));
    }

    #[test]
    fn the_empty_string_and_empty_array_are_true() {
        // A surprise worth pinning: MongoDB treats only false, null, missing
        // and zero as false.
        assert!(truthy(&Bson::String(String::new())));
        assert!(truthy(&Bson::Array(Vec::new())));
        assert!(!truthy(&Bson::Int64(0)));
        assert!(!truthy(&Bson::Double(0.0)));
        assert!(!truthy(&Bson::Null));
    }

    // -- dates ------------------------------------------------------------

    #[test]
    fn date_parts_come_out_in_utc() {
        // 2026-08-12T13:45:07.250Z
        let d = doc! {"t": dt(1_786_542_307_250)};
        assert_eq!(on(doc! {"$year": "$t"}.into(), d.clone()), Bson::Int32(2026));
        assert_eq!(on(doc! {"$month": "$t"}.into(), d.clone()), Bson::Int32(8));
        assert_eq!(on(doc! {"$dayOfMonth": "$t"}.into(), d.clone()), Bson::Int32(12));
        assert_eq!(on(doc! {"$hour": "$t"}.into(), d.clone()), Bson::Int32(13));
        assert_eq!(on(doc! {"$minute": "$t"}.into(), d.clone()), Bson::Int32(45));
        assert_eq!(on(doc! {"$second": "$t"}.into(), d), Bson::Int32(7));
    }

    #[test]
    fn the_epoch_and_a_leap_day_are_right() {
        assert_eq!(Civil::from_millis(0).year, 1970);
        assert_eq!(Civil::from_millis(0).month, 1);
        assert_eq!(Civil::from_millis(0).day, 1);

        // 2024-02-29T00:00:00Z — a leap day, the case the cycle arithmetic
        // exists to get right.
        let leap = Civil::from_millis(1_709_164_800_000);
        assert_eq!((leap.year, leap.month, leap.day), (2024, 2, 29));
    }

    #[test]
    fn dates_before_the_epoch_do_not_land_a_day_late() {
        // Floor division rather than truncation: 1969-12-31T23:59:59Z.
        let before = Civil::from_millis(-1_000);
        assert_eq!((before.year, before.month, before.day), (1969, 12, 31));
        assert_eq!((before.hour, before.minute, before.second), (23, 59, 59));
    }

    #[test]
    fn date_to_string_formats_the_supported_specifiers() {
        let d = doc! {"t": dt(1_786_542_307_250)};
        assert_eq!(
            on(doc! {"$dateToString": {"date": "$t", "format": "%Y-%m-%d"}}.into(), d.clone()),
            Bson::String("2026-08-12".into())
        );
        assert_eq!(
            on(doc! {"$dateToString": {"date": "$t", "format": "%H:%M:%S.%L"}}.into(), d.clone()),
            Bson::String("13:45:07.250".into())
        );
        assert_eq!(
            on(doc! {"$dateToString": {"date": "$t", "format": "100%%"}}.into(), d),
            Bson::String("100%".into())
        );
    }

    #[test]
    fn date_to_string_defaults_to_iso8601() {
        assert_eq!(
            on(doc! {"$dateToString": {"date": "$t"}}.into(), doc! {"t": dt(0)}),
            Bson::String("1970-01-01T00:00:00.000Z".into())
        );
    }

    #[test]
    fn an_unknown_date_specifier_is_refused_not_copied_through() {
        // A literal `%q` in every row of a report is the kind of wrong output
        // nobody notices.
        let expr = doc! {"$dateToString": {"date": "$t", "format": "%q"}};
        assert!(ev(expr.into(), &doc! {"t": dt(0)}).is_err());
        let trailing = doc! {"$dateToString": {"date": "$t", "format": "ends with %"}};
        assert!(ev(trailing.into(), &doc! {"t": dt(0)}).is_err());
    }

    #[test]
    fn date_operators_refuse_a_non_date_and_pass_null_through() {
        assert!(ev(doc! {"$year": 5}.into(), &Document::new()).is_err());
        assert_eq!(on(doc! {"$year": "$gone"}.into(), doc! {}), Bson::Null);
    }

    // -- nesting ----------------------------------------------------------

    #[test]
    fn expressions_nest_arbitrarily() {
        let expr = doc! {
            "$concat": [
                {"$toUpper": "$first"},
                " ",
                {"$cond": [{"$gte": ["$score", 50]}, "PASS", "FAIL"]},
            ]
        };
        assert_eq!(
            on(expr.clone().into(), doc! {"first": "ada", "score": 90}),
            Bson::String("ADA PASS".into())
        );
        assert_eq!(
            on(expr.into(), doc! {"first": "bob", "score": 10}),
            Bson::String("BOB FAIL".into())
        );
    }

    #[test]
    fn a_dotted_path_reaches_into_a_subdocument() {
        assert_eq!(
            on(doc! {"$toUpper": "$user.name"}.into(), doc! {"user": {"name": "ada"}}),
            Bson::String("ADA".into())
        );
    }

    // -- the $sum accumulator's exactness ---------------------------------

    #[test]
    fn total_sums_large_integers_exactly() {
        // The regression this type exists for. Accumulating in f64 gives
        // 9007199254740992 for the pair below, because 2^53 + 1 has no f64
        // representation and the addition rounds away the carry.
        let mut t = Total::default();
        t.add(&Bson::Int64(9_007_199_254_740_993));
        t.add(&Bson::Int64(1));
        assert_eq!(t.to_bson(), Bson::Int64(9_007_199_254_740_994));
    }

    #[test]
    fn total_widens_once_a_double_arrives() {
        let mut t = Total::default();
        t.add(&Bson::Int64(1));
        t.add(&Bson::Double(0.5));
        assert_eq!(t.to_bson(), Bson::Double(1.5));
    }

    #[test]
    fn total_ignores_non_numbers_and_starts_at_zero() {
        let mut t = Total::default();
        t.add(&Bson::String("nope".into()));
        assert_eq!(t.to_bson(), Bson::Int64(0));
        t.add(&Bson::Int32(3));
        assert_eq!(t.to_bson(), Bson::Int64(3));
    }

    #[test]
    fn total_promotes_on_overflow_rather_than_wrapping() {
        let mut t = Total::default();
        t.add(&Bson::Int64(i64::MAX));
        t.add(&Bson::Int64(i64::MAX));
        match t.to_bson() {
            Bson::Double(d) => assert!(d > 1.8e19),
            other => panic!("expected a double, got {other:?}"),
        }
    }
}
