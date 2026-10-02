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
//! - a string starting with `$$` is a **variable** — `$$ROOT`, `$$CURRENT`, or
//!   a name bound by an enclosing `$let`, `$map`, `$filter`, `$reduce` or
//!   `$lookup` `let` — optionally followed by a dot path into its value;
//! - any other string starting with `$` is a **field path**;
//! - a document whose **first key starts with `$`** is an **operator**, and its
//!   keys may not mix operator and plain names;
//! - any other document is an **object expression** — its values are
//!   expressions and the result is a document. This is what makes a compound
//!   `$group` key work;
//! - an array is an **array expression** — each element is an expression,
//!   parsed in the same scope, and the result is the array of their values
//!   (ADR-215). `{$literal: [...]}` keeps an array as written;
//! - anything else is a literal.
//!
//! That third rule is a **behaviour change**: `{_id: {a: "$x"}}` used to be a
//! constant document, so every input landed in one bucket. It now computes.
//! [`Op::Literal`] is the escape hatch, and exists because the rule needs one —
//! without it there is no way to produce the string `"$x"`.
//!
//! # Scope
//!
//! Evaluation happens in a [`Scope`]: the root document plus a chain of frames,
//! one per enclosing construct that binds a name. A variable reference searches
//! the innermost frame first and walks outward, so an inner `as` shadows an
//! outer one exactly as a lexical scope does, and a name no enclosing construct
//! binds is refused when the expression is **parsed** rather than evaluating to
//! null per document. That one mechanism is what admits the whole array family:
//! `$$this` is not special-cased per operator, it is a binding like any other
//! (ADR-105).
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
    // Arrays
    Size,
    ArrayElemAt,
    First,
    Last,
    Slice,
    ConcatArrays,
    In,
    IndexOfArray,
    IsArray,
    ReverseArray,
    Range,
    // Sets
    SetUnion,
    SetIntersection,
    SetDifference,
    SetEquals,
    SetIsSubset,
    AnyElementTrue,
    AllElementsTrue,
    // Documents and arrays of pairs
    ObjectToArray,
    ArrayToObject,
    // Escape
    Literal,
}

/// How many arguments an operator accepts.
#[derive(Clone, Copy, Debug)]
enum Arity {
    Exact(usize),
    AtLeast(usize),
    Between(usize, usize),
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
            "$size" => Op::Size,
            "$arrayElemAt" => Op::ArrayElemAt,
            "$first" => Op::First,
            "$last" => Op::Last,
            "$slice" => Op::Slice,
            "$concatArrays" => Op::ConcatArrays,
            "$in" => Op::In,
            "$indexOfArray" => Op::IndexOfArray,
            "$isArray" => Op::IsArray,
            "$reverseArray" => Op::ReverseArray,
            "$range" => Op::Range,
            "$setUnion" => Op::SetUnion,
            "$setIntersection" => Op::SetIntersection,
            "$setDifference" => Op::SetDifference,
            "$setEquals" => Op::SetEquals,
            "$setIsSubset" => Op::SetIsSubset,
            "$anyElementTrue" => Op::AnyElementTrue,
            "$allElementsTrue" => Op::AllElementsTrue,
            "$objectToArray" => Op::ObjectToArray,
            "$arrayToObject" => Op::ArrayToObject,
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
            Op::Size => "$size",
            Op::ArrayElemAt => "$arrayElemAt",
            Op::First => "$first",
            Op::Last => "$last",
            Op::Slice => "$slice",
            Op::ConcatArrays => "$concatArrays",
            Op::In => "$in",
            Op::IndexOfArray => "$indexOfArray",
            Op::IsArray => "$isArray",
            Op::ReverseArray => "$reverseArray",
            Op::Range => "$range",
            Op::SetUnion => "$setUnion",
            Op::SetIntersection => "$setIntersection",
            Op::SetDifference => "$setDifference",
            Op::SetEquals => "$setEquals",
            Op::SetIsSubset => "$setIsSubset",
            Op::AnyElementTrue => "$anyElementTrue",
            Op::AllElementsTrue => "$allElementsTrue",
            Op::ObjectToArray => "$objectToArray",
            Op::ArrayToObject => "$arrayToObject",
            Op::Literal => "$literal",
        }
    }

    fn arity(self) -> Arity {
        match self {
            Op::Add
            | Op::Multiply
            | Op::Concat
            | Op::And
            | Op::Or
            | Op::ConcatArrays
            | Op::SetUnion
            | Op::SetIntersection => Arity::AtLeast(1),
            Op::SetEquals => Arity::AtLeast(2),
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
            | Op::Cmp
            | Op::ArrayElemAt
            | Op::In
            | Op::SetDifference
            | Op::SetIsSubset => Arity::Exact(2),
            Op::Substr => Arity::Exact(3),
            Op::Cond => Arity::Exact(3),
            Op::Slice | Op::Range => Arity::Between(2, 3),
            Op::IndexOfArray => Arity::Between(2, 4),
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
            | Op::Size
            | Op::First
            | Op::Last
            | Op::IsArray
            | Op::ReverseArray
            | Op::AnyElementTrue
            | Op::AllElementsTrue
            | Op::ObjectToArray
            | Op::ArrayToObject
            | Op::Literal => Arity::Exact(1),
        }
    }

    /// Whether the operator wants its arguments evaluated before it runs.
    ///
    /// `$cond` and `$ifNull` are the exceptions and must not be: evaluating
    /// every branch would make `{$cond: [{$gt: ["$n", 0]}, {$divide: [1,
    /// "$n"]}, 0]}` fail on exactly the inputs the guard exists to protect.
    /// `$and` and `$or` stop at the argument that decides them, and an
    /// argument that cannot be evaluated fails them only when none does
    /// (ADR-211). `$literal` does not evaluate its argument at all.
    fn is_lazy(self) -> bool {
        matches!(self, Op::Cond | Op::IfNull | Op::And | Op::Or | Op::Literal)
    }
}

/// The type a `$convert` produces.
///
/// The seven of MongoDB's targets the engine can hold. `decimal` is refused at
/// parse: `Decimal128` has no exact key encoding here (ADR-005), so a value
/// converted to it could be neither indexed nor grouped, and producing one
/// would only move the refusal somewhere less obvious.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConvertTo {
    Double,
    String,
    ObjectId,
    Bool,
    Date,
    Int,
    Long,
}

/// Refuse a `Decimal128` literal, wherever an expression would hold one.
///
/// The reason `$convert` refuses `decimal` as a target, met from the other
/// side: a Decimal128 has no exact key encoding here (ADR-005) and ranks
/// equal to every other number in `canonical_cmp`, so a comparison against
/// one would hold for every number and a value computed from one could be
/// neither indexed nor grouped. Recursive, because a document or array
/// literal is compared by its contents.
fn refuse_decimal_literal(value: &Bson) -> Result<()> {
    if kimmy_core::holds_decimal128(value) {
        return Err(Error::InvalidQuery(
            "a Decimal128 literal is not supported in an expression: it has no exact key \
             encoding in this engine and ranks equal to every other number, so nothing compared \
             with it or computed from it could be exact; write a double or a long instead"
                .into(),
        ));
    }
    Ok(())
}

impl ConvertTo {
    /// `to` as written: a type name or its numeric BSON code, both as `$type`
    /// spells them.
    fn parse(value: &Bson) -> Result<Self> {
        let name = match value {
            Bson::String(s) => s.as_str(),
            Bson::Int32(1) | Bson::Int64(1) => "double",
            Bson::Int32(2) | Bson::Int64(2) => "string",
            Bson::Int32(7) | Bson::Int64(7) => "objectId",
            Bson::Int32(8) | Bson::Int64(8) => "bool",
            Bson::Int32(9) | Bson::Int64(9) => "date",
            Bson::Int32(16) | Bson::Int64(16) => "int",
            Bson::Int32(18) | Bson::Int64(18) => "long",
            Bson::Int32(19) | Bson::Int64(19) => "decimal",
            Bson::Int32(_) | Bson::Int64(_) => {
                return Err(Error::InvalidQuery(format!(
                    "$convert `to` code {value} is not a convertible type; use 1 (double), 2 \
                     (string), 7 (objectId), 8 (bool), 9 (date), 16 (int) or 18 (long)"
                )));
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$convert `to` is a type name or numeric code, found {}",
                    type_name(other)
                )));
            }
        };
        Ok(match name {
            "double" => ConvertTo::Double,
            "string" => ConvertTo::String,
            "objectId" => ConvertTo::ObjectId,
            "bool" => ConvertTo::Bool,
            "date" => ConvertTo::Date,
            "int" => ConvertTo::Int,
            "long" => ConvertTo::Long,
            "decimal" => {
                return Err(Error::InvalidQuery(
                    "$convert to decimal is not supported: Decimal128 has no exact key encoding \
                     in this engine, so the result could be neither indexed nor grouped; convert \
                     to double or long instead"
                        .into(),
                ));
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$convert cannot target {other:?}; supported: double, string, objectId, \
                     bool, date, int, long"
                )));
            }
        })
    }

    /// The `$toX` shorthand that fixes this target, if the name is one.
    fn from_shorthand(name: &str) -> Option<Self> {
        Some(match name {
            "$toDouble" => ConvertTo::Double,
            "$toString" => ConvertTo::String,
            "$toObjectId" => ConvertTo::ObjectId,
            "$toBool" => ConvertTo::Bool,
            "$toDate" => ConvertTo::Date,
            "$toInt" => ConvertTo::Int,
            "$toLong" => ConvertTo::Long,
            _ => return None,
        })
    }

    /// The type name, for error messages.
    pub fn name(self) -> &'static str {
        match self {
            ConvertTo::Double => "double",
            ConvertTo::String => "string",
            ConvertTo::ObjectId => "objectId",
            ConvertTo::Bool => "bool",
            ConvertTo::Date => "date",
            ConvertTo::Int => "int",
            ConvertTo::Long => "long",
        }
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
    /// `"$$name"` or `"$$name.path"` — a variable from the [`Scope`], read into
    /// with a dot path when one is given.
    Var { name: String, path: Option<String> },
    /// Any other BSON value, used as-is.
    Literal(Bson),
    /// An operator over positional arguments.
    Op(Op, Vec<Expr>),
    /// `{$switch: {branches: [{case, then}, ...], default: <expr>}}`.
    Switch { branches: Vec<(Expr, Expr)>, default: Option<Box<Expr>> },
    /// `{$dateToString: {date: <expr>, format: "<fmt>"}}`.
    DateToString { date: Box<Expr>, format: String },
    /// `{$convert: {input, to, onError?, onNull?}}`, and the `$toX` shorthands,
    /// which are the same node with no fallbacks.
    ///
    /// `on_null` answers a null or missing input; `on_error` answers a value
    /// that has no conversion. Neither is evaluated unless it is needed, so a
    /// fallback may itself be an expression that would fail on other rows.
    Convert {
        input: Box<Expr>,
        to: ConvertTo,
        on_error: Option<Box<Expr>>,
        on_null: Option<Box<Expr>>,
    },
    /// `{$let: {vars: {name: <expr>, ...}, in: <expr>}}`.
    Let { vars: Vec<(String, Expr)>, body: Box<Expr> },
    /// `{$filter: {input: <expr>, as: "name", cond: <expr>, limit: <expr>}}`.
    Filter { input: Box<Expr>, as_name: String, cond: Box<Expr>, limit: Option<Box<Expr>> },
    /// `{$map: {input: <expr>, as: "name", in: <expr>}}`.
    Map { input: Box<Expr>, as_name: String, body: Box<Expr> },
    /// `{$reduce: {input: <expr>, initialValue: <expr>, in: <expr>}}` — `in`
    /// sees `$$value` and `$$this`.
    Reduce { input: Box<Expr>, initial: Box<Expr>, body: Box<Expr> },
    /// A document whose values are expressions.
    Object(Vec<(String, Expr)>),
    /// An array whose elements are expressions, evaluated in order to the
    /// array of their values. An array whose elements are all constants, at
    /// any depth, is folded to one [`Expr::Literal`] at parse, which answers
    /// the same.
    Array(Vec<Expr>),
}

// ---------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------

/// A variable bound in a [`Scope`]: its name and its value.
pub type Binding<'a> = (&'a str, &'a Bson);

/// What an expression can see while it evaluates.
///
/// The root document and a chain of frames. `$let`, `$map`, `$filter` and
/// `$reduce` each push a frame holding the names they bind and evaluate their
/// body in it; a reference searches the innermost frame first and walks
/// outward, so an inner binding shadows an outer one of the same name. The
/// chain is borrowed rather than copied — pushing a frame per array element
/// costs a stack slot and nothing else.
#[derive(Clone, Copy, Debug)]
pub struct Scope<'a> {
    root: &'a Document,
    bindings: &'a [Binding<'a>],
    /// Whether `bindings` is in name order, so a lookup in it can bisect: a frame
    /// of more than [`BISECT_FRAME`] names is built that way, and a smaller one
    /// is scanned.
    sorted: bool,
    parent: Option<&'a Scope<'a>>,
}

/// The size of a frame past which its names are kept in order and bisected, so
/// that reading each of a `$let`'s thousands of variables is not a walk of them
/// all. Below it a scan is cheaper than a sort, and every frame but a `$let`'s is
/// one or two names.
const BISECT_FRAME: usize = 16;

impl<'a> Scope<'a> {
    /// A scope over a document with nothing bound beyond `$$ROOT` and
    /// `$$CURRENT` — what every stage outside a `$lookup` sub-pipeline wants.
    pub fn new(root: &'a Document) -> Self {
        Self { root, bindings: &[], sorted: false, parent: None }
    }

    /// A scope over a document with variables already in place — what a
    /// `$lookup` sub-pipeline runs its stages in, its `let` bound.
    ///
    /// A later binding shadows an earlier one of the same name, so a caller
    /// layering an inner set over an outer one appends rather than prepends.
    pub fn with_bindings(root: &'a Document, bindings: &'a [Binding<'a>]) -> Self {
        Self { root, bindings, sorted: false, parent: None }
    }

    /// The document `$$ROOT` names.
    pub fn root(&self) -> &'a Document {
        self.root
    }

    fn nested<'b>(&'b self, bindings: &'b [Binding<'b>]) -> Scope<'b> {
        Scope { root: self.root, bindings, sorted: false, parent: Some(self) }
    }

    /// [`Self::nested`] over a frame already in name order, which a lookup in it
    /// bisects. Names in a frame are distinct, so the order is a total one.
    fn nested_sorted<'b>(&'b self, bindings: &'b [Binding<'b>]) -> Scope<'b> {
        Scope { root: self.root, bindings, sorted: true, parent: Some(self) }
    }

    fn get(&self, name: &str) -> Option<&'a Bson> {
        let found = if self.sorted {
            self.bindings
                .binary_search_by(|(bound, _)| {
                    #[cfg(test)]
                    FRAME_COMPARISONS.with(|n| n.set(n.get() + 1));
                    (*bound).cmp(name)
                })
                .ok()
                .map(|at| self.bindings[at].1)
        } else {
            self.bindings
                .iter()
                .rev()
                .find(|(bound, _)| {
                    #[cfg(test)]
                    FRAME_COMPARISONS.with(|n| n.set(n.get() + 1));
                    *bound == name
                })
                .map(|(_, value)| *value)
        };
        found.or_else(|| self.parent.and_then(|parent| parent.get(name)))
    }
}

/// The variables bound without being asked. `$$CURRENT` is `$$ROOT`: nothing
/// here rebinds it, because `$map` and its relatives bind `$$this` instead.
const SYSTEM_VARIABLES: [&str; 2] = ["ROOT", "CURRENT"];

/// A user variable is a lowercase ASCII letter followed by letters, digits
/// and underscores. MongoDB's rule, kept so a pipeline written for it parses
/// here — and so a user name can never collide with a system one, which are
/// all uppercase.
pub(crate) fn validate_variable_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let ok = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidQuery(format!(
            "variable name {name:?} must start with a lowercase letter and contain only letters, \
             digits and underscores"
        )))
    }
}

thread_local! {
    /// The names a [`FreeVariables`] parse has met unbound, while one is running.
    static FREE_VARIABLES: std::cell::RefCell<Option<std::collections::BTreeSet<String>>> =
        const { std::cell::RefCell::new(None) };
    /// Expression parses begun on this thread, for the tests that hold a
    /// search to one.
    #[cfg(test)]
    static PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Scope lookups made by [`Declared::binds`] on this thread, for the tests
    /// that hold a parse to one probe per reference.
    #[cfg(test)]
    static LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Names compared by [`Scope::get`] on this thread, for the tests that hold
    /// reading a `$let`'s variables to a bisect and not a walk.
    #[cfg(test)]
    static FRAME_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A scope, for the life of the guard, in which the parser on this thread
/// records an unbound lowercase `$$name` instead of refusing it. Parsing is
/// synchronous, so the state is the thread's and the guard clears it.
struct FreeVariables;

impl FreeVariables {
    fn begin() -> Self {
        FREE_VARIABLES.with(|c| *c.borrow_mut() = Some(std::collections::BTreeSet::new()));
        FreeVariables
    }

    /// Record `name` if a collecting parse is running; whether one is.
    fn record(name: &str) -> bool {
        FREE_VARIABLES.with(|c| match c.borrow_mut().as_mut() {
            Some(names) => {
                names.insert(name.to_string());
                true
            }
            None => false,
        })
    }

    fn take() -> Vec<String> {
        FREE_VARIABLES.with(|c| c.borrow_mut().take().unwrap_or_default().into_iter().collect())
    }
}

impl Drop for FreeVariables {
    fn drop(&mut self) {
        FREE_VARIABLES.with(|c| *c.borrow_mut() = None);
    }
}

/// The lexical environment of a parse: the names in scope, innermost last, and
/// how many bindings of each are in scope, so that asking whether a name is
/// bound is one hash lookup and not a walk of everything bound (ADR-105). A
/// `$let` with thousands of variables, each read once, was quadratic here, on
/// the request thread, where the request timeout cannot interrupt it.
///
/// A name bound again by an inner construct is counted twice, so leaving the
/// inner one leaves the outer one bound, which is what shadowing is.
struct Declared {
    stack: Vec<std::rc::Rc<str>>,
    bound: BoundNames,
}

/// How many enclosing bindings each name has; the one thing a lookup consults.
#[derive(Default)]
struct BoundNames(std::collections::HashMap<std::rc::Rc<str>, usize>);

impl BoundNames {
    /// One probe.
    fn contains(&self, name: &str) -> bool {
        #[cfg(test)]
        LOOKUPS.with(|n| n.set(n.get() + 1));
        self.0.contains_key(name)
    }
}

impl Declared {
    fn new(vars: &[String]) -> Self {
        let mut declared = Declared { stack: Vec::new(), bound: BoundNames::default() };
        for name in vars {
            declared.push(name);
        }
        declared
    }

    fn depth(&self) -> usize {
        self.stack.len()
    }

    fn push(&mut self, name: &str) {
        let name: std::rc::Rc<str> = name.into();
        self.stack.push(std::rc::Rc::clone(&name));
        *self.bound.0.entry(name).or_insert(0) += 1;
    }

    /// Leave every binding made since the scope was `depth` deep.
    fn truncate(&mut self, depth: usize) {
        while self.stack.len() > depth {
            let name = self.stack.pop().expect("longer than depth");
            if let Some(count) = self.bound.0.get_mut(&*name) {
                *count -= 1;
                if *count == 0 {
                    self.bound.0.remove(&name);
                }
            }
        }
    }

    /// Whether `name` is bound by an enclosing construct. One probe.
    fn binds(&self, name: &str) -> bool {
        self.bound.contains(name)
    }
}

impl Expr {
    /// Parse an expression from BSON.
    ///
    /// Only `$$ROOT` and `$$CURRENT` are in scope; a `$$name` nothing binds is
    /// an error here rather than a null later.
    pub fn parse(value: &Bson) -> Result<Self> {
        Self::parse_with_vars(value, &[])
    }

    /// Parse with `vars` already bound by an enclosing construct — a `$lookup`
    /// `let`, whose names the sub-pipeline's expressions may use.
    pub fn parse_with_vars(value: &Bson, vars: &[String]) -> Result<Self> {
        #[cfg(test)]
        PARSES.with(|n| n.set(n.get() + 1));
        let mut declared = Declared::new(vars);
        Self::parse_in(value, &mut declared)
    }

    /// The user variables `value` reads that nothing inside it binds, in name
    /// order: what an enclosing construct has to bind for it to parse.
    ///
    /// Found by one parse, under the parser's own scoping: in this mode a
    /// `$$name` nothing binds is recorded, where a plain parse refuses it, so a
    /// name `$let`, `$map`, `$filter` or `$reduce` binds around a use of it is
    /// not free there, and one bound around one use and not around another is.
    /// A string a `$literal` holds is not read as a reference at all. An
    /// expression that does not parse for another reason is that error.
    pub fn free_variables(value: &Bson) -> Result<Vec<String>> {
        let _collecting = FreeVariables::begin();
        Self::parse_with_vars(value, &[])?;
        Ok(FreeVariables::take())
    }

    /// Whether evaluating this reads the document it is evaluated against:
    /// a field (`"$qty"`) or `$$ROOT` or `$$CURRENT` anywhere in the tree.
    /// An expression for which this is false depends on its bound variables
    /// and its literals alone.
    pub fn reads_document(&self) -> bool {
        let any = |exprs: &[Expr]| exprs.iter().any(Expr::reads_document);
        match self {
            Expr::Field(_) => true,
            Expr::Var { name, .. } => SYSTEM_VARIABLES.contains(&name.as_str()),
            Expr::Literal(_) => false,
            Expr::Op(_, args) => any(args),
            Expr::Switch { branches, default } => {
                branches.iter().any(|(case, then)| case.reads_document() || then.reads_document())
                    || default.as_ref().is_some_and(|d| d.reads_document())
            }
            Expr::DateToString { date, .. } => date.reads_document(),
            Expr::Convert { input, on_error, on_null, .. } => {
                input.reads_document()
                    || on_error.as_ref().is_some_and(|e| e.reads_document())
                    || on_null.as_ref().is_some_and(|e| e.reads_document())
            }
            Expr::Let { vars, body } => {
                vars.iter().any(|(_, e)| e.reads_document()) || body.reads_document()
            }
            Expr::Filter { input, cond, limit, .. } => {
                input.reads_document()
                    || cond.reads_document()
                    || limit.as_ref().is_some_and(|l| l.reads_document())
            }
            Expr::Map { input, body, .. } => input.reads_document() || body.reads_document(),
            Expr::Reduce { input, initial, body } => {
                input.reads_document() || initial.reads_document() || body.reads_document()
            }
            Expr::Object(fields) => fields.iter().any(|(_, e)| e.reads_document()),
            Expr::Array(items) => any(items),
        }
    }

    /// `{name: <expr>, ...}` — the `vars` of `$let` and the `let` of `$lookup`.
    ///
    /// The values are parsed against `vars`, the enclosing scope: one binding
    /// cannot see its sibling, only what was in scope around the whole
    /// construct. Names are validated here so both callers refuse the same
    /// things.
    pub fn parse_bindings(raw: &Document, vars: &[String]) -> Result<Vec<(String, Expr)>> {
        let mut declared = Declared::new(vars);
        Self::parse_bindings_in(raw, &mut declared)
    }

    /// `declared` is the lexical environment: every construct that binds a
    /// name pushes it before parsing its body and pops it after, so an unknown
    /// variable is caught where it is written.
    fn parse_in(value: &Bson, declared: &mut Declared) -> Result<Self> {
        match value {
            Bson::String(s) => Ok(match s.strip_prefix('$') {
                Some(rest) if rest.starts_with('$') => Self::parse_variable(&rest[1..], declared)?,
                Some(field) if !field.is_empty() => Expr::Field(field.to_string()),
                // A bare string without `$` is a literal, which is what
                // MongoDB does: `{$sum: "total"}` sums the constant.
                _ => Expr::Literal(value.clone()),
            }),
            Bson::Document(doc) => Self::parse_document(doc, declared),
            Bson::Array(items) => Self::parse_array(items, declared),
            other => {
                refuse_decimal_literal(other)?;
                Ok(Expr::Literal(other.clone()))
            }
        }
    }

    /// `name` or `name.path`, after the `$$`.
    ///
    /// Refused unless something binds the name. Parsing `$$ROOT` as a field
    /// called `$ROOT` — which is what a naive reading does — would silently
    /// yield null in every row, and a typo in `$$this` deserves the same
    /// refusal as a typo in an operator name.
    fn parse_variable(spec: &str, declared: &Declared) -> Result<Self> {
        let (name, path) = match spec.split_once('.') {
            Some((name, path)) => (name, Some(path)),
            None => (spec, None),
        };
        if name.is_empty() {
            return Err(Error::InvalidQuery("a variable needs a name after $$".into()));
        }
        if path.is_some_and(str::is_empty) {
            return Err(Error::InvalidQuery(format!("$${name}. needs a field path after the dot")));
        }
        if !SYSTEM_VARIABLES.contains(&name) && !declared.binds(name) {
            // Collecting free variables: a lowercase name nothing binds is the
            // answer, not an error. An uppercase one is a system variable this
            // build does not have, and is refused below as ever.
            if validate_variable_name(name).is_ok() && FreeVariables::record(name) {
                return Ok(Expr::Var { name: name.to_string(), path: path.map(str::to_string) });
            }
            // MongoDB's other system variables — `$$NOW`, `$$REMOVE`,
            // `$$DESCEND` and the rest — are uppercase by rule, so an
            // uppercase name that is not bound is a feature this does not
            // have rather than a typo, and the message says which.
            if name.starts_with(|c: char| c.is_ascii_uppercase()) {
                return Err(Error::UnsupportedOperator {
                    operator: format!("$${name}"),
                    reason: Some(
                        "a system variable this build does not have; $$ROOT and $$CURRENT are \
                         the ones it does"
                            .into(),
                    ),
                });
            }
            return Err(Error::InvalidQuery(format!(
                "unknown variable $${name}; the variables in scope are $$ROOT, $$CURRENT and any \
                 bound by an enclosing $let, $map, $filter, $reduce or $lookup let"
            )));
        }
        Ok(Expr::Var { name: name.to_string(), path: path.map(str::to_string) })
    }

    /// An array written inside an expression: each element is an expression,
    /// parsed in the scope the array sits in, so a field path, a variable
    /// and an operator in it are read rather than kept as written.
    ///
    /// The operand list of an operator is not this: `parse_args` reads that
    /// list itself, and only an array nested as one operand, or written where
    /// a single expression goes, arrives here. `{$literal: [...]}` never does.
    ///
    /// When every element is a literal the array is folded to one literal,
    /// which evaluates to the same value without a walk per document. A
    /// document of literals is one already (see [`Self::parse_document`]), so
    /// an array of constants folds at any depth.
    fn parse_array(items: &[Bson], declared: &mut Declared) -> Result<Self> {
        let elements =
            items.iter().map(|v| Self::parse_in(v, declared)).collect::<Result<Vec<_>>>()?;
        if elements.iter().all(|e| matches!(e, Expr::Literal(_))) {
            let values = elements
                .into_iter()
                .map(|e| match e {
                    Expr::Literal(v) => v,
                    _ => unreachable!("every element was checked to be a literal"),
                })
                .collect();
            return Ok(Expr::Literal(Bson::Array(values)));
        }
        Ok(Expr::Array(elements))
    }

    fn parse_document(doc: &Document, declared: &mut Declared) -> Result<Self> {
        let Some((first, _)) = doc.iter().next() else {
            // `{}` is an empty document, not an operator.
            return Ok(Expr::Literal(Bson::Document(Document::new())));
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
                fields.push((key.clone(), Self::parse_in(value, declared)?));
            }
            // A document whose values are all literals is one literal: the
            // same document in every row, built once.
            if fields.iter().all(|(_, e)| matches!(e, Expr::Literal(_))) {
                let folded = fields
                    .into_iter()
                    .map(|(key, e)| match e {
                        Expr::Literal(v) => (key, v),
                        _ => unreachable!("every value was checked to be a literal"),
                    })
                    .collect();
                return Ok(Expr::Literal(Bson::Document(folded)));
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
            "$switch" => Self::parse_switch(raw, declared),
            "$dateToString" => Self::parse_date_to_string(raw, declared),
            "$let" => Self::parse_let(raw, declared),
            "$filter" => Self::parse_filter(raw, declared),
            "$map" => Self::parse_map(raw, declared),
            "$reduce" => Self::parse_reduce(raw, declared),
            "$convert" => Self::parse_convert(raw, declared),
            name if ConvertTo::from_shorthand(name).is_some() => {
                Self::parse_convert_shorthand(name, raw, declared)
            }
            name => {
                let Some(op) = Op::from_name(name) else {
                    return Err(Error::UnsupportedOperator {
                        operator: name.to_string(),
                        reason: Some("not an expression operator".into()),
                    });
                };
                if op == Op::Literal {
                    refuse_decimal_literal(raw)?;
                    return Ok(Expr::Op(op, vec![Expr::Literal(raw.clone())]));
                }
                let args = Self::parse_args(op, raw, declared)?;
                Ok(Expr::Op(op, args))
            }
        }
    }

    /// Arguments are an array, or a single value when the operator takes one.
    ///
    /// MongoDB allows `{$toUpper: "$name"}` as well as `{$toUpper: ["$name"]}`,
    /// and the shorthand is what people actually write.
    fn parse_args(op: Op, raw: &Bson, declared: &mut Declared) -> Result<Vec<Expr>> {
        let args = match raw {
            Bson::Array(items) => {
                items.iter().map(|v| Self::parse_in(v, declared)).collect::<Result<Vec<_>>>()?
            }
            single => vec![Self::parse_in(single, declared)?],
        };

        let ok = match op.arity() {
            Arity::Exact(n) => args.len() == n,
            Arity::AtLeast(n) => args.len() >= n,
            Arity::Between(lo, hi) => (lo..=hi).contains(&args.len()),
        };
        if !ok {
            let want = match op.arity() {
                Arity::Exact(n) => format!("exactly {n}"),
                Arity::AtLeast(n) => format!("at least {n}"),
                Arity::Between(lo, hi) => format!("{lo} to {hi}"),
            };
            return Err(Error::InvalidQuery(format!(
                "{} takes {want} argument(s), found {}",
                op.name(),
                args.len()
            )));
        }
        Ok(args)
    }

    fn parse_switch(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let spec = Self::named_spec("$switch", raw, &["branches", "default"])?;
        let Some(Bson::Array(raw_branches)) = spec.get("branches") else {
            return Err(Error::InvalidQuery("$switch needs a `branches` array".into()));
        };
        if raw_branches.is_empty() {
            return Err(Error::InvalidQuery("$switch needs at least one branch".into()));
        }

        let mut branches = Vec::with_capacity(raw_branches.len());
        for branch in raw_branches {
            let b = Self::named_spec("a $switch branch", branch, &["case", "then"])?;
            let (Some(case), Some(then)) = (b.get("case"), b.get("then")) else {
                return Err(Error::InvalidQuery(
                    "each $switch branch needs `case` and `then`".into(),
                ));
            };
            branches.push((Self::parse_in(case, declared)?, Self::parse_in(then, declared)?));
        }

        let default = match spec.get("default") {
            Some(d) => Some(Box::new(Self::parse_in(d, declared)?)),
            None => None,
        };
        Ok(Expr::Switch { branches, default })
    }

    fn parse_date_to_string(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let spec = Self::named_spec("$dateToString", raw, &["date", "format"])?;
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
        Ok(Expr::DateToString { date: Box::new(Self::parse_in(date, declared)?), format })
    }

    // -- the binding operators ------------------------------------------

    /// The document a named-argument operator takes, checked for keys it does
    /// not know: `{$filter: {input: …, condition: …}}` should fail on the
    /// typo, not silently keep every element.
    fn named_spec<'a>(op: &str, raw: &'a Bson, allowed: &[&str]) -> Result<&'a Document> {
        let Bson::Document(spec) = raw else {
            return Err(Error::InvalidQuery(format!(
                "{op} takes a document, found {}",
                type_name(raw)
            )));
        };
        for key in spec.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(Error::InvalidQuery(format!(
                    "{op} does not take `{key}`; it takes {}",
                    allowed.join(", ")
                )));
            }
        }
        Ok(spec)
    }

    fn required<'a>(op: &str, spec: &'a Document, key: &str) -> Result<&'a Bson> {
        spec.get(key).ok_or_else(|| Error::InvalidQuery(format!("{op} needs `{key}`")))
    }

    /// `as` names the element variable and defaults to `this`, as in MongoDB.
    fn as_name(op: &str, spec: &Document) -> Result<String> {
        match spec.get("as") {
            None => Ok("this".to_string()),
            Some(Bson::String(name)) => {
                validate_variable_name(name)?;
                Ok(name.clone())
            }
            Some(other) => Err(Error::InvalidQuery(format!(
                "{op} `as` is a string, found {}",
                type_name(other)
            ))),
        }
    }

    /// Parse `body` with `names` bound, and unbind them after.
    fn parse_scoped<'n>(
        body: &Bson,
        names: impl IntoIterator<Item = &'n str>,
        declared: &mut Declared,
    ) -> Result<Self> {
        let depth = declared.depth();
        for name in names {
            declared.push(name);
        }
        let parsed = Self::parse_in(body, declared);
        declared.truncate(depth);
        parsed
    }

    fn parse_bindings_in(raw: &Document, declared: &mut Declared) -> Result<Vec<(String, Expr)>> {
        raw.iter()
            .map(|(name, value)| {
                validate_variable_name(name)?;
                Ok((name.clone(), Self::parse_in(value, declared)?))
            })
            .collect()
    }

    fn parse_let(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let spec = Self::named_spec("$let", raw, &["vars", "in"])?;
        let Bson::Document(raw_vars) = Self::required("$let", spec, "vars")? else {
            return Err(Error::InvalidQuery(
                "$let `vars` is a document of name: expression".into(),
            ));
        };
        let body = Self::required("$let", spec, "in")?;
        // The values are parsed before the names are declared, so a value
        // cannot see its sibling — the same rule evaluation follows.
        let vars = Self::parse_bindings_in(raw_vars, declared)?;
        let body = Self::parse_scoped(body, vars.iter().map(|(n, _)| n.as_str()), declared)?;
        Ok(Expr::Let { vars, body: Box::new(body) })
    }

    fn parse_filter(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let spec = Self::named_spec("$filter", raw, &["input", "as", "cond", "limit"])?;
        let input = Self::parse_in(Self::required("$filter", spec, "input")?, declared)?;
        let limit = match spec.get("limit") {
            Some(l) => Some(Box::new(Self::parse_in(l, declared)?)),
            None => None,
        };
        let as_name = Self::as_name("$filter", spec)?;
        let cond = Self::required("$filter", spec, "cond")?;
        let cond = Self::parse_scoped(cond, [as_name.as_str()], declared)?;
        Ok(Expr::Filter { input: Box::new(input), as_name, cond: Box::new(cond), limit })
    }

    fn parse_map(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let spec = Self::named_spec("$map", raw, &["input", "as", "in"])?;
        let input = Self::parse_in(Self::required("$map", spec, "input")?, declared)?;
        let as_name = Self::as_name("$map", spec)?;
        let body = Self::required("$map", spec, "in")?;
        let body = Self::parse_scoped(body, [as_name.as_str()], declared)?;
        Ok(Expr::Map { input: Box::new(input), as_name, body: Box::new(body) })
    }

    fn parse_reduce(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let spec = Self::named_spec("$reduce", raw, &["input", "initialValue", "in"])?;
        let input = Self::parse_in(Self::required("$reduce", spec, "input")?, declared)?;
        let initial = Self::parse_in(Self::required("$reduce", spec, "initialValue")?, declared)?;
        let body = Self::required("$reduce", spec, "in")?;
        let body = Self::parse_scoped(body, ["value", "this"], declared)?;
        Ok(Expr::Reduce {
            input: Box::new(input),
            initial: Box::new(initial),
            body: Box::new(body),
        })
    }

    /// `{$convert: {input: <expr>, to: <type>, onError?: <expr>, onNull?: <expr>}}`.
    ///
    /// `to` is a constant. MongoDB lets it be an expression; nothing here
    /// needs a per-document target type, and a constant is what lets the
    /// unsupported `decimal` be refused before a document is read. A key this
    /// does not know is an error rather than being ignored: `onerror` for
    /// `onError` would otherwise silently mean "no fallback".
    fn parse_convert(raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let Bson::Document(spec) = raw else {
            return Err(Error::InvalidQuery(format!(
                "$convert takes a document, found {}",
                type_name(raw)
            )));
        };
        for key in spec.keys() {
            if !matches!(key.as_str(), "input" | "to" | "onError" | "onNull") {
                return Err(Error::InvalidQuery(format!(
                    "$convert does not take {key:?}; it takes input, to, onError and onNull"
                )));
            }
        }
        let Some(input) = spec.get("input") else {
            return Err(Error::InvalidQuery("$convert needs an `input`".into()));
        };
        let Some(to) = spec.get("to") else {
            return Err(Error::InvalidQuery(
                "$convert needs a `to`: a type name such as \"int\" or its numeric code".into(),
            ));
        };
        let optional = |key: &str, declared: &mut Declared| -> Result<Option<Box<Expr>>> {
            spec.get(key).map(|v| Self::parse_in(v, declared).map(Box::new)).transpose()
        };
        Ok(Expr::Convert {
            input: Box::new(Self::parse_in(input, declared)?),
            to: ConvertTo::parse(to)?,
            on_error: optional("onError", declared)?,
            on_null: optional("onNull", declared)?,
        })
    }

    /// `{$toInt: <expr>}` and its siblings: a `$convert` with the target
    /// fixed and no fallbacks. Takes the single value or a one-element array,
    /// as every other one-argument operator does.
    fn parse_convert_shorthand(name: &str, raw: &Bson, declared: &mut Declared) -> Result<Self> {
        let to = ConvertTo::from_shorthand(name).expect("checked by the caller");
        let arg = match raw {
            Bson::Array(items) if items.len() == 1 => &items[0],
            Bson::Array(items) => {
                return Err(Error::InvalidQuery(format!(
                    "{name} takes exactly 1 argument, found {}",
                    items.len()
                )));
            }
            single => single,
        };
        Ok(Expr::Convert {
            input: Box::new(Self::parse_in(arg, declared)?),
            to,
            on_error: None,
            on_null: None,
        })
    }

    /// Resolve against a document.
    ///
    /// Nothing is bound beyond `$$ROOT` and `$$CURRENT`, which is what every
    /// caller outside a `$lookup` sub-pipeline wants. A missing field is
    /// `Null`, matching how the filter layer treats absence.
    pub fn eval(&self, doc: &Document) -> Result<Bson> {
        self.eval_in(&Scope::new(doc))
    }

    /// Resolve in a scope that may already bind variables.
    pub fn eval_in(&self, scope: &Scope<'_>) -> Result<Bson> {
        match self {
            Expr::Field(p) => Ok(field_path(scope.root, p).unwrap_or(Bson::Null)),
            Expr::Var { name, path } => eval_variable(name, path.as_deref(), scope),
            Expr::Literal(v) => Ok(v.clone()),
            Expr::Object(fields) => {
                let mut out = Document::new();
                for (key, expr) in fields {
                    out.insert(key.clone(), expr.eval_in(scope)?);
                }
                Ok(Bson::Document(out))
            }
            // A missing field is null in its place, as everywhere else: the
            // element is kept, so positions do not shift. The first element
            // that fails fails the array, with its own error, which is a
            // value error like any other and may be held by `$and`/`$or`.
            Expr::Array(items) => {
                items.iter().map(|e| e.eval_in(scope)).collect::<Result<Vec<_>>>().map(Bson::Array)
            }
            Expr::Switch { branches, default } => {
                for (case, then) in branches {
                    if truthy(&case.eval_in(scope)?) {
                        return then.eval_in(scope);
                    }
                }
                match default {
                    Some(d) => d.eval_in(scope),
                    None => Err(Error::InvalidQuery(
                        "no $switch branch matched and there is no `default`".into(),
                    )),
                }
            }
            Expr::DateToString { date, format } => match date.eval_in(scope)? {
                Bson::Null => Ok(Bson::Null),
                Bson::DateTime(dt) => Ok(Bson::String(format_date(dt.timestamp_millis(), format)?)),
                other => Err(Error::InvalidQuery(format!(
                    "$dateToString needs a date, found {}",
                    type_name(&other)
                ))),
            },
            Expr::Convert { input, to, on_error, on_null } => {
                // An error in the input expression itself is not a conversion
                // error and is not caught by `onError`: a fallback is for a
                // value that cannot be converted, not for a broken expression.
                let value = input.eval_in(scope)?;
                if matches!(value, Bson::Null | Bson::Undefined) {
                    return match on_null {
                        Some(fallback) => fallback.eval_in(scope),
                        None => Ok(Bson::Null),
                    };
                }
                match convert(&value, *to) {
                    Ok(converted) => Ok(converted),
                    Err(e) => match on_error {
                        Some(fallback) => fallback.eval_in(scope),
                        None => Err(e),
                    },
                }
            }
            Expr::Let { vars, body } => {
                // Values are evaluated in the enclosing scope, then bound
                // together: `{a: 1, b: "$$a"}` is an error at parse, not 1.
                let values =
                    vars.iter().map(|(_, e)| e.eval_in(scope)).collect::<Result<Vec<_>>>()?;
                let mut frame: Vec<Binding<'_>> =
                    vars.iter().zip(&values).map(|((name, _), v)| (name.as_str(), v)).collect();
                if frame.len() > BISECT_FRAME {
                    frame.sort_unstable_by(|a, b| a.0.cmp(b.0));
                    return body.eval_in(&scope.nested_sorted(&frame));
                }
                body.eval_in(&scope.nested(&frame))
            }
            Expr::Filter { input, as_name, cond, limit } => {
                eval_filter(input, as_name, cond, limit.as_deref(), scope)
            }
            Expr::Map { input, as_name, body } => eval_map(input, as_name, body, scope),
            Expr::Reduce { input, initial, body } => eval_reduce(input, initial, body, scope),
            Expr::Op(op, args) if op.is_lazy() => eval_lazy(*op, args, scope),
            Expr::Op(op, args) => {
                let values = args.iter().map(|a| a.eval_in(scope)).collect::<Result<Vec<_>>>()?;
                eval_op(*op, &values)
            }
        }
    }
}

/// The value a field path names when it is read as an **expression**.
///
/// This is not [`path::resolve`]. That function serves the filter language,
/// where `{"items.sku": "a"}` asks whether *any* element matches, so it
/// returns every value the path reaches and lets a numeric segment mean
/// either an index or a field name. An expression asks for *the value*, and
/// MongoDB's aggregation field-path rules give one answer:
///
/// - Segments are walked left to right. A segment that lands on a document
///   reads the field. The last segment's value is returned as it is, array
///   or not: `$tags` over `tags: ["a", "b"]` is `["a", "b"]`.
/// - A segment that lands on an **array** before the path has ended applies
///   the rest of the path to each element that is a document and collects
///   the results into a new array. An element that is not a document, or
///   in which the rest of the path is missing, contributes nothing — it is
///   skipped, not filled with null. So `$items.sku` over
///   `items: [{sku: "a"}, {sku: "b"}]` is `["a", "b"]`, and over
///   `items: [{sku: "a"}, 7, {}]` it is `["a"]`.
/// - Each array crossed produces one array; nothing is flattened further.
///   `$a.b` over `a: [{b: [1, 2]}, {b: 3}]` is `[[1, 2], 3]`, because the
///   last segment returns each `b` as it is, and `$a.b.c` over
///   `a: [{b: [{c: 1}, {c: 2}]}]` is `[[1, 2]]`, because the inner array is
///   crossed inside the outer one's single element. An array nested directly
///   inside an array is not a document and is skipped.
/// - A **numeric segment is a field name**, never an index. `$items.0.sku`
///   reads the field called `0` of each element and finds nothing unless an
///   element has one; positional access is `$arrayElemAt`. The filter
///   language honours both readings, and that is the one place the two path
///   rules disagree.
///
/// `None` is a *missing* value: the top-level field is absent, a segment
/// before the last lands on a scalar, or the last segment names a field the
/// document lacks. Once an array has been crossed nothing is missing — an
/// array none of whose elements had the field is `[]`. Every caller here maps
/// `None` to `Bson::Null`, which is the one place this departs from MongoDB:
/// a `$project` or `$addFields` of a missing path there omits the field, and
/// here writes null — an expression always yields a value, as the array
/// operators' entry in `deviations.md` records.
fn field_path(doc: &Document, p: &str) -> Option<Bson> {
    field_path_segments(doc, &path::segments(p))
}

fn field_path_segments(doc: &Document, segs: &[&str]) -> Option<Bson> {
    let (head, rest) = segs.split_first()?;
    let value = doc.get(*head)?;
    if rest.is_empty() {
        return Some(value.clone());
    }
    match value {
        Bson::Document(inner) => field_path_segments(inner, rest),
        Bson::Array(items) => Some(Bson::Array(fan_out(items, rest))),
        _ => None,
    }
}

/// The rest of a path applied to each document element of an array crossed
/// midway — the one rule in [`field_path`] that produces an array.
fn fan_out(items: &[Bson], rest: &[&str]) -> Vec<Bson> {
    items
        .iter()
        .filter_map(|item| match item {
            Bson::Document(doc) => field_path_segments(doc, rest),
            _ => None,
        })
        .collect()
}

/// A path read into a variable's value, by the rules of [`field_path`].
///
/// A variable is whatever its expression evaluated to, so unlike the root it
/// can be an array: `$$this.sku` inside a `$map` over an array of documents
/// reads one document, but `$$rows.sku` under a `$let` that bound the whole
/// array fans out over it exactly as `$rows.sku` would. Anything else — a
/// number, a string, null — has no fields, and a path into it is missing,
/// exactly what `$a.b` is when `a` holds a number.
fn value_path(value: &Bson, p: &str) -> Option<Bson> {
    let segs = path::segments(p);
    match value {
        Bson::Document(doc) => field_path_segments(doc, &segs),
        Bson::Array(items) => Some(Bson::Array(fan_out(items, &segs))),
        _ => None,
    }
}

fn eval_variable(name: &str, path: Option<&str>, scope: &Scope<'_>) -> Result<Bson> {
    if SYSTEM_VARIABLES.contains(&name) {
        return Ok(match path {
            None => Bson::Document(scope.root.clone()),
            // `$$ROOT.items.sku` is `$items.sku` spelled out.
            Some(p) => field_path(scope.root, p).unwrap_or(Bson::Null),
        });
    }
    let Some(value) = scope.get(name) else {
        // Parsing declares every name before reading its body, so this is a
        // caller evaluating with fewer bindings than it parsed with: a bug,
        // which an `$or` with a true argument must not hide (ADR-211).
        return Err(Error::Internal(format!("variable $${name} is not bound")));
    };
    Ok(match path {
        None => value.clone(),
        Some(p) => value_path(value, p).unwrap_or(Bson::Null),
    })
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// The operators that must not have their arguments pre-evaluated.
fn eval_lazy(op: Op, args: &[Expr], scope: &Scope<'_>) -> Result<Bson> {
    match op {
        Op::Literal => match args {
            [inner] => inner.eval_in(scope),
            _ => unreachable!("arity checked at parse"),
        },
        Op::Cond => match args {
            [cond, then, otherwise] => {
                if truthy(&cond.eval_in(scope)?) {
                    then.eval_in(scope)
                } else {
                    otherwise.eval_in(scope)
                }
            }
            _ => unreachable!("arity checked at parse"),
        },
        Op::IfNull => match args {
            [value, fallback] => {
                let v = value.eval_in(scope)?;
                if matches!(v, Bson::Null | Bson::Undefined) {
                    fallback.eval_in(scope)
                } else {
                    Ok(v)
                }
            }
            _ => unreachable!("arity checked at parse"),
        },
        // The filter level's `$and` and `$or` run through the same two
        // functions, so the two levels cannot come to read an error
        // differently (ADR-206, ADR-211). The answer is a boolean, never the
        // argument that decided it.
        Op::And => all_of(args, |arg| truth_of(arg, scope)).map(Bson::Boolean),
        Op::Or => any_of(args, |arg| truth_of(arg, scope)).map(Bson::Boolean),
        _ => unreachable!("only lazy operators reach here"),
    }
}

/// One argument of an `$and` or `$or`, read for its truth.
fn truth_of(arg: &Expr, scope: &Scope<'_>) -> Result<bool> {
    #[cfg(test)]
    LOGIC_ARGUMENTS.with(|n| n.set(n.get() + 1));
    arg.eval_in(scope).map(|v| truthy(&v))
}

#[cfg(test)]
thread_local! {
    /// How many `$and`/`$or` arguments this thread has evaluated, for the
    /// tests that hold the two to stopping at the argument that decides.
    static LOGIC_ARGUMENTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// `$and`/`$or` arguments this thread has evaluated so far.
#[cfg(test)]
pub(crate) fn logic_arguments() -> usize {
    LOGIC_ARGUMENTS.with(std::cell::Cell::get)
}

/// Three-valued conjunction: whether every item is true, where an item may
/// also be not known (an `Err`). Shared by the expression language's `$and`
/// and the filter level's (ADR-206, ADR-211), so both read one truth table.
///
/// - **Any `false` decides** the answer, `false`, whatever came before or
///   after it; the scan stops there.
/// - Otherwise, **if any item is not known, so is the answer**, and the error
///   returned is that of the earliest-written item that has one. Items are
///   evaluated in the order given, and an error is held, not returned, until
///   the scan ends: the expression level passes its arguments as written, and
///   the filter level's cheap-first order moves forward only clauses that
///   cannot fail, keeping the rest as written. So the first error met is the
///   earliest written. A caller that reorders items that can fail must keep
///   the error of the lowest written position instead.
/// - Otherwise the answer is `true`.
///
/// An error that is not [deferrable](Error::is_deferrable) — a broken
/// invariant, or one day a budget for the whole request — is returned at
/// once and nothing after it is evaluated.
pub(crate) fn all_of<T>(
    items: impl IntoIterator<Item = T>,
    f: impl FnMut(T) -> Result<bool>,
) -> Result<bool> {
    decide(items, f, false)
}

/// Three-valued disjunction: [`all_of`] with `true` and `false` swapped. Any
/// `true` decides; otherwise the earliest-written error; otherwise `false`.
pub(crate) fn any_of<T>(
    items: impl IntoIterator<Item = T>,
    f: impl FnMut(T) -> Result<bool>,
) -> Result<bool> {
    decide(items, f, true)
}

/// The scan behind [`all_of`] and [`any_of`]: the first item whose truth is
/// `decider` decides; an error waits for one.
fn decide<T>(
    items: impl IntoIterator<Item = T>,
    mut f: impl FnMut(T) -> Result<bool>,
    decider: bool,
) -> Result<bool> {
    let mut undecided = None;
    for item in items {
        match f(item) {
            Ok(v) if v == decider => return Ok(decider),
            Ok(_) => {}
            Err(e) if !e.is_deferrable() => return Err(e),
            Err(e) => {
                undecided.get_or_insert(e);
            }
        }
    }
    undecided.map_or(Ok(!decider), Err)
}

/// The array an iteration operator walks, or `None` when the input is null
/// and the whole result should be. A non-array is refused: `$map` over a
/// string has no meaning, and returning null would hide the type error.
fn iteration_input(op: &str, value: Bson) -> Result<Option<Vec<Bson>>> {
    match value {
        Bson::Null | Bson::Undefined => Ok(None),
        Bson::Array(items) => Ok(Some(items)),
        other => Err(Error::InvalidQuery(format!(
            "{op} needs an array as its input, found {}",
            type_name(&other)
        ))),
    }
}

fn eval_filter(
    input: &Expr,
    as_name: &str,
    cond: &Expr,
    limit: Option<&Expr>,
    scope: &Scope<'_>,
) -> Result<Bson> {
    let Some(items) = iteration_input("$filter", input.eval_in(scope)?)? else {
        return Ok(Bson::Null);
    };
    // A null limit means no limit, as in MongoDB. Zero or a negative number is
    // refused: neither can be what a caller meant, and an empty result would
    // look exactly like a condition nothing satisfied.
    let limit = match limit {
        None => None,
        Some(l) => match l.eval_in(scope)? {
            Bson::Null | Bson::Undefined => None,
            v => {
                let n = as_integer(&v, "$filter limit")?;
                if n < 1 {
                    return Err(Error::InvalidQuery(format!(
                        "$filter limit must be a positive number, found {n}"
                    )));
                }
                Some(n as usize)
            }
        },
    };

    let mut out = Vec::new();
    for item in &items {
        if limit.is_some_and(|n| out.len() >= n) {
            break;
        }
        let frame = [(as_name, item)];
        if truthy(&cond.eval_in(&scope.nested(&frame))?) {
            out.push(item.clone());
        }
    }
    Ok(Bson::Array(out))
}

fn eval_map(input: &Expr, as_name: &str, body: &Expr, scope: &Scope<'_>) -> Result<Bson> {
    let Some(items) = iteration_input("$map", input.eval_in(scope)?)? else {
        return Ok(Bson::Null);
    };
    let mut out = Vec::with_capacity(items.len());
    for item in &items {
        let frame = [(as_name, item)];
        out.push(body.eval_in(&scope.nested(&frame))?);
    }
    Ok(Bson::Array(out))
}

fn eval_reduce(input: &Expr, initial: &Expr, body: &Expr, scope: &Scope<'_>) -> Result<Bson> {
    let Some(items) = iteration_input("$reduce", input.eval_in(scope)?)? else {
        return Ok(Bson::Null);
    };
    // The initial value is evaluated in the enclosing scope, once: it may not
    // refer to `$$this`, and there is no element for it to refer to.
    let mut acc = initial.eval_in(scope)?;
    for item in &items {
        let frame = [("value", &acc), ("this", item)];
        let next = body.eval_in(&scope.nested(&frame))?;
        acc = next;
    }
    Ok(acc)
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

        Op::Not => Ok(Bson::Boolean(!truthy(&args[0]))),

        Op::Year | Op::Month | Op::DayOfMonth | Op::Hour | Op::Minute | Op::Second => {
            date_part(op, &args[0])
        }

        Op::Size => array_size(&args[0]),
        Op::ArrayElemAt => array_elem_at(&args[0], &args[1]),
        Op::First | Op::Last => array_end(op, &args[0]),
        Op::Slice => array_slice(args),
        Op::ConcatArrays => concat_arrays(args),
        Op::In => array_in(&args[0], &args[1]),
        Op::IndexOfArray => index_of_array(args),
        Op::IsArray => Ok(Bson::Boolean(matches!(args[0], Bson::Array(_)))),
        Op::ReverseArray => reverse_array(&args[0]),
        Op::Range => range(args),

        Op::SetUnion => set_union(args),
        Op::SetIntersection => set_intersection(args),
        Op::SetDifference => set_difference(args),
        Op::SetEquals => set_equals(args),
        Op::SetIsSubset => set_is_subset(args),
        Op::AnyElementTrue | Op::AllElementsTrue => element_truth(op, &args[0]),
        Op::ObjectToArray => object_to_array(&args[0]),
        Op::ArrayToObject => array_to_object(&args[0]),

        Op::Cond | Op::IfNull | Op::And | Op::Or | Op::Literal => {
            unreachable!("handled lazily")
        }
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
// Arrays
// ---------------------------------------------------------------------------

/// The array an operator works on, or `None` when it is null and the result
/// should be too. A non-array is refused — the same rule as the numeric
/// operators: absence propagates, a wrong type does not pass silently.
fn as_array(op: Op, value: &Bson) -> Result<Option<&[Bson]>> {
    match value {
        Bson::Null | Bson::Undefined => Ok(None),
        Bson::Array(items) => Ok(Some(items)),
        other => Err(Error::InvalidQuery(format!(
            "{} needs an array, found {}",
            op.name(),
            type_name(other)
        ))),
    }
}

/// A whole number from any numeric type.
///
/// A double with a fraction is refused rather than truncated:
/// `{$arrayElemAt: ["$xs", 1.5]}` is a mistake, not a request for element 1.
fn as_integer(value: &Bson, what: &str) -> Result<i64> {
    match value {
        Bson::Int32(n) => Ok(i64::from(*n)),
        Bson::Int64(n) => Ok(*n),
        Bson::Double(d) if d.is_finite() && d.fract() == 0.0 => Ok(*d as i64),
        other => Err(Error::InvalidQuery(format!(
            "{what} must be a whole number, found {}",
            type_name(other)
        ))),
    }
}

/// [`as_integer`], with null passed through so the caller can return null.
fn integer_or_null(value: &Bson, what: &str) -> Result<Option<i64>> {
    match value {
        Bson::Null | Bson::Undefined => Ok(None),
        other => as_integer(other, what).map(Some),
    }
}

fn as_count(value: &Bson, what: &str) -> Result<usize> {
    let n = as_integer(value, what)?;
    usize::try_from(n)
        .map_err(|_| Error::InvalidQuery(format!("{what} cannot be negative, found {n}")))
}

/// Resolve a possibly negative index against a length: `-1` is the last
/// element. `None` when it falls outside on either side.
fn position(len: usize, index: i64) -> Option<usize> {
    let len = len as i64;
    let p = if index < 0 { index + len } else { index };
    (0..len).contains(&p).then_some(p as usize)
}

fn array_size(value: &Bson) -> Result<Bson> {
    Ok(match as_array(Op::Size, value)? {
        None => Bson::Null,
        Some(items) => Bson::Int64(items.len() as i64),
    })
}

/// Out of range on either side is null, which is as near to MongoDB's
/// "missing" as a value can come — see `docs/deviations.md`.
fn array_elem_at(array: &Bson, index: &Bson) -> Result<Bson> {
    let Some(items) = as_array(Op::ArrayElemAt, array)? else {
        return Ok(Bson::Null);
    };
    let Some(i) = integer_or_null(index, "$arrayElemAt index")? else {
        return Ok(Bson::Null);
    };
    Ok(position(items.len(), i).and_then(|p| items.get(p)).cloned().unwrap_or(Bson::Null))
}

fn array_end(op: Op, value: &Bson) -> Result<Bson> {
    let Some(items) = as_array(op, value)? else {
        return Ok(Bson::Null);
    };
    let picked = match op {
        Op::First => items.first(),
        Op::Last => items.last(),
        _ => unreachable!("only $first and $last reach here"),
    };
    Ok(picked.cloned().unwrap_or(Bson::Null))
}

/// `[array, n]` takes the first `n`, or the last `|n|` when `n` is negative.
/// `[array, position, n]` takes `n` from `position`, counted from the end when
/// negative. Both are MongoDB's readings, and a window past either end is
/// empty rather than an error, as `$substr` past the end is.
fn array_slice(args: &[Bson]) -> Result<Bson> {
    let Some(items) = as_array(Op::Slice, &args[0])? else {
        return Ok(Bson::Null);
    };
    let len = items.len();
    let (start, count) = match args {
        [_, n] => {
            let Some(n) = integer_or_null(n, "$slice count")? else {
                return Ok(Bson::Null);
            };
            if n < 0 {
                (len.saturating_sub(n.unsigned_abs() as usize), len)
            } else {
                (0, n.unsigned_abs() as usize)
            }
        }
        [_, pos, n] => {
            let (Some(pos), Some(n)) =
                (integer_or_null(pos, "$slice position")?, integer_or_null(n, "$slice count")?)
            else {
                return Ok(Bson::Null);
            };
            if n < 1 {
                return Err(Error::InvalidQuery(format!(
                    "$slice count must be positive when a position is given, found {n}; the \
                     two-argument form takes from the end"
                )));
            }
            let start = if pos < 0 {
                len.saturating_sub(pos.unsigned_abs() as usize)
            } else {
                (pos.unsigned_abs() as usize).min(len)
            };
            (start, n.unsigned_abs() as usize)
        }
        _ => unreachable!("arity checked at parse"),
    };
    Ok(Bson::Array(items.iter().skip(start).take(count).cloned().collect()))
}

fn concat_arrays(args: &[Bson]) -> Result<Bson> {
    let mut out = Vec::new();
    for arg in args {
        match as_array(Op::ConcatArrays, arg)? {
            // One null makes the whole result null, as `$concat` does: a
            // missing array must not silently vanish from the middle.
            None => return Ok(Bson::Null),
            Some(items) => out.extend_from_slice(items),
        }
    }
    Ok(Bson::Array(out))
}

/// Membership by the canonical order, so `5` is in `[5.0]` — the same equality
/// `$eq` and the indexes use.
fn array_in(value: &Bson, array: &Bson) -> Result<Bson> {
    let Some(items) = as_array(Op::In, array)? else {
        return Ok(Bson::Null);
    };
    Ok(Bson::Boolean(items.iter().any(|item| canonical_cmp(item, value) == Ordering::Equal)))
}

/// `[array, value, start?, end?]` — the first index of `value` in
/// `array[start..end]`, or `-1`.
fn index_of_array(args: &[Bson]) -> Result<Bson> {
    let Some(items) = as_array(Op::IndexOfArray, &args[0])? else {
        return Ok(Bson::Null);
    };
    let needle = &args[1];
    let start = match args.get(2) {
        None => 0,
        Some(Bson::Null | Bson::Undefined) => return Ok(Bson::Null),
        Some(v) => as_count(v, "$indexOfArray start")?,
    };
    let end = match args.get(3) {
        None => items.len(),
        Some(Bson::Null | Bson::Undefined) => return Ok(Bson::Null),
        Some(v) => as_count(v, "$indexOfArray end")?.min(items.len()),
    };
    let found = items
        .iter()
        .enumerate()
        .take(end)
        .skip(start)
        .find(|(_, item)| canonical_cmp(item, needle) == Ordering::Equal)
        .map(|(i, _)| i as i64);
    Ok(Bson::Int64(found.unwrap_or(-1)))
}

fn reverse_array(value: &Bson) -> Result<Bson> {
    Ok(match as_array(Op::ReverseArray, value)? {
        None => Bson::Null,
        Some(items) => Bson::Array(items.iter().rev().cloned().collect()),
    })
}

/// The most elements `$range` will build.
///
/// The same figure as the pipeline's document ceiling, for the same reason:
/// `{$range: [0, 1000000000]}` is a memory exhaustion written as an
/// expression, and refusing it is better than becoming slow.
pub const MAX_RANGE_LENGTH: usize = 100_000;

/// `[start, end, step?]` — integers from `start` up to but excluding `end`.
fn range(args: &[Bson]) -> Result<Bson> {
    let mut nums = Vec::with_capacity(3);
    for (arg, what) in args.iter().zip(["$range start", "$range end", "$range step"]) {
        match integer_or_null(arg, what)? {
            None => return Ok(Bson::Null),
            Some(n) => nums.push(n),
        }
    }
    let (start, end) = (nums[0], nums[1]);
    let step = nums.get(2).copied().unwrap_or(1);
    if step == 0 {
        return Err(Error::InvalidQuery("$range step cannot be zero".into()));
    }

    // The length is decided before anything is allocated: the point of the
    // cap is to stop the allocation, not to report it afterwards.
    let span = if step > 0 { end.saturating_sub(start) } else { start.saturating_sub(end) };
    let count = if span <= 0 { 0 } else { span.unsigned_abs().div_ceil(step.unsigned_abs()) };
    if count > MAX_RANGE_LENGTH as u64 {
        return Err(Error::InvalidQuery(format!(
            "$range would produce {count} elements, over the limit of {MAX_RANGE_LENGTH}"
        )));
    }
    Ok(Bson::Array((0..count as i64).map(|i| Bson::Int64(start + i * step)).collect()))
}

// ---------------------------------------------------------------------------
// Sets
// ---------------------------------------------------------------------------
//
// The set operators read each array as the set of its distinct members, and
// two values are one member exactly when `$eq` says they are equal: when
// `canonical_cmp` ranks them `Equal`. So `1`, `1.0` and `1_i64` are one
// member, every `NaN` is one, `0.0` and `-0.0` are one, `null` and
// `undefined` are one, and two documents are one only with their keys in the
// same order. A `Decimal128` anywhere in an input is refused, because that
// order ranks one equal to every other number (ADR-207).
//
// A result is in **first-seen order**: the members as they first appear,
// reading the arguments left to right and each array from its start, and the
// member kept is that first appearance, untouched — `$setUnion` of `[1]` and
// `[1.0]` is `[1]`, still an `Int32`. Nothing here builds more than its inputs
// already hold, so no cap of its own applies; `$range`'s exists because it
// builds from two integers.

/// A value ordered by [`canonical_cmp`], so an ordered set of them holds one
/// entry per member. With every `Decimal128` refused before one is built, the
/// order is total and its `Equal` is an equivalence.
struct Member<'a>(&'a Bson);

impl PartialEq for Member<'_> {
    fn eq(&self, other: &Self) -> bool {
        canonical_cmp(self.0, other.0) == Ordering::Equal
    }
}

impl Eq for Member<'_> {}

impl PartialOrd for Member<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Member<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        canonical_cmp(self.0, other.0)
    }
}

type Members<'a> = std::collections::BTreeSet<Member<'a>>;

/// The arrays a set operator compares, or `None` when one of them is null and
/// so is the result.
///
/// Every argument's type is checked before a null is honoured, so a
/// non-array is refused wherever it stands; a set has no first argument that
/// could hide the others. A `Decimal128` anywhere in an array is refused once
/// no argument is null — the point where it would be compared.
fn set_inputs(op: Op, args: &[Bson]) -> Result<Option<Vec<&[Bson]>>> {
    let mut arrays = Vec::with_capacity(args.len());
    let mut null = false;
    for arg in args {
        match as_array(op, arg)? {
            None => null = true,
            Some(items) => arrays.push(items),
        }
    }
    if null {
        return Ok(None);
    }
    for (i, items) in arrays.iter().enumerate() {
        if items.iter().any(kimmy_core::holds_decimal128) {
            return Err(Error::InvalidQuery(format!(
                "{} cannot compare a Decimal128 (argument {} holds one): the canonical order \
                 ranks a Decimal128 equal to every other number, so set membership would be \
                 wrong; store a double or a long",
                op.name(),
                i + 1
            )));
        }
    }
    Ok(Some(arrays))
}

fn members(items: &[Bson]) -> Members<'_> {
    items.iter().map(Member).collect()
}

/// Every member of every array, first-seen.
fn set_union(args: &[Bson]) -> Result<Bson> {
    let Some(arrays) = set_inputs(Op::SetUnion, args)? else {
        return Ok(Bson::Null);
    };
    let mut seen = Members::new();
    let mut out = Vec::new();
    for item in arrays.into_iter().flatten() {
        if seen.insert(Member(item)) {
            out.push(item.clone());
        }
    }
    Ok(Bson::Array(out))
}

/// The members of the first array that every other array holds, first-seen
/// in the first.
fn set_intersection(args: &[Bson]) -> Result<Bson> {
    let Some(arrays) = set_inputs(Op::SetIntersection, args)? else {
        return Ok(Bson::Null);
    };
    let (first, rest) = arrays.split_first().expect("arity checked at parse");
    let others: Vec<Members<'_>> = rest.iter().map(|items| members(items)).collect();
    let mut seen = Members::new();
    let mut out = Vec::new();
    for item in *first {
        if others.iter().all(|set| set.contains(&Member(item))) && seen.insert(Member(item)) {
            out.push(item.clone());
        }
    }
    Ok(Bson::Array(out))
}

/// The members of the first array the second does not hold, first-seen in
/// the first.
fn set_difference(args: &[Bson]) -> Result<Bson> {
    let Some(arrays) = set_inputs(Op::SetDifference, args)? else {
        return Ok(Bson::Null);
    };
    let excluded = members(arrays[1]);
    let mut seen = Members::new();
    let mut out = Vec::new();
    for item in arrays[0] {
        if !excluded.contains(&Member(item)) && seen.insert(Member(item)) {
            out.push(item.clone());
        }
    }
    Ok(Bson::Array(out))
}

/// Whether every array has the same members, duplicates and order aside.
fn set_equals(args: &[Bson]) -> Result<Bson> {
    let Some(arrays) = set_inputs(Op::SetEquals, args)? else {
        return Ok(Bson::Null);
    };
    let first = members(arrays[0]);
    Ok(Bson::Boolean(arrays[1..].iter().all(|items| members(items) == first)))
}

/// Whether every member of the first array is a member of the second.
fn set_is_subset(args: &[Bson]) -> Result<Bson> {
    let Some(arrays) = set_inputs(Op::SetIsSubset, args)? else {
        return Ok(Bson::Null);
    };
    let superset = members(arrays[1]);
    Ok(Bson::Boolean(arrays[0].iter().all(|item| superset.contains(&Member(item)))))
}

/// `$anyElementTrue` and `$allElementsTrue`: the array's elements read by
/// [`truthy`], as `$and` and `$or` read their arguments. An empty array has
/// no true element and no false one, so it is `false` and `true`.
///
/// A `Decimal128` element is read like any number: false when it is a zero.
/// Unlike the set operators, nothing here compares two values, so the
/// canonical order's trouble with a `Decimal128` does not arise.
fn element_truth(op: Op, value: &Bson) -> Result<Bson> {
    let Some(items) = as_array(op, value)? else {
        return Ok(Bson::Null);
    };
    Ok(Bson::Boolean(match op {
        Op::AnyElementTrue => items.iter().any(truthy),
        Op::AllElementsTrue => items.iter().all(truthy),
        _ => unreachable!("only $anyElementTrue and $allElementsTrue reach here"),
    }))
}

// ---------------------------------------------------------------------------
// Documents and arrays of pairs
// ---------------------------------------------------------------------------

/// A document as `[{k, v}, ...]`, in the document's field order. Only the top
/// level is turned over; a value that is itself a document stays one.
fn object_to_array(value: &Bson) -> Result<Bson> {
    match value {
        Bson::Null | Bson::Undefined => Ok(Bson::Null),
        Bson::Document(fields) => Ok(Bson::Array(
            fields
                .iter()
                .map(|(k, v)| {
                    let mut pair = Document::new();
                    pair.insert("k", k.as_str());
                    pair.insert("v", v.clone());
                    Bson::Document(pair)
                })
                .collect(),
        )),
        other => Err(Error::InvalidQuery(format!(
            "$objectToArray needs a document, found {}",
            type_name(other)
        ))),
    }
}

/// The two spellings of one key-value pair `$arrayToObject` reads.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PairForm {
    /// `[key, value]`
    Array,
    /// `{k: key, v: value}`
    Document,
}

impl PairForm {
    fn describe(self) -> &'static str {
        match self {
            PairForm::Array => "a [key, value] array",
            PairForm::Document => "a {k, v} document",
        }
    }
}

/// `[[key, value], ...]` or `[{k: key, v: value}, ...]` as a document.
///
/// The first element decides the form and every other element must use it. A
/// key is any string without a NUL, which a BSON field name cannot hold: an
/// empty, dotted or `$`-prefixed key is taken as written, as an object
/// expression, `$literal` and a stored document already take one, so
/// `$objectToArray` and back is the same document whatever its keys. A key
/// that comes again keeps the place of its first appearance and takes the
/// value of its last. Anything else is refused, naming the element by its
/// index.
fn array_to_object(value: &Bson) -> Result<Bson> {
    let Some(items) = as_array(Op::ArrayToObject, value)? else {
        return Ok(Bson::Null);
    };
    let mut out = Document::new();
    let mut form = None;
    for (i, item) in items.iter().enumerate() {
        let (this, key, value) = match item {
            Bson::Array(pair) => match pair.as_slice() {
                [key, value] => (PairForm::Array, key, value),
                _ => {
                    return Err(Error::InvalidQuery(format!(
                        "$arrayToObject element {i} is an array of {} elements; a [key, value] \
                         pair has exactly 2",
                        pair.len()
                    )));
                }
            },
            Bson::Document(pair) => {
                if let Some(extra) = pair.keys().find(|k| *k != "k" && *k != "v") {
                    return Err(Error::InvalidQuery(format!(
                        "$arrayToObject element {i} has a field {extra:?} besides `k` and `v`"
                    )));
                }
                let (Some(key), Some(value)) = (pair.get("k"), pair.get("v")) else {
                    return Err(Error::InvalidQuery(format!(
                        "$arrayToObject element {i} needs both `k` and `v`"
                    )));
                };
                (PairForm::Document, key, value)
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$arrayToObject element {i} is {}; each element is a [key, value] array or \
                     a {{k, v}} document",
                    type_name(other)
                )));
            }
        };
        match form {
            None => form = Some(this),
            Some(first) if first != this => {
                return Err(Error::InvalidQuery(format!(
                    "$arrayToObject cannot mix the two forms: element 0 is {}, element {i} is {}",
                    first.describe(),
                    this.describe()
                )));
            }
            Some(_) => {}
        }
        let Bson::String(key) = key else {
            return Err(Error::InvalidQuery(format!(
                "$arrayToObject element {i} has a key that is {}; a key must be a string",
                type_name(key)
            )));
        };
        if key.contains('\0') {
            return Err(Error::InvalidQuery(format!(
                "$arrayToObject element {i} has the key {key:?}, which holds a NUL; a BSON field \
                 name cannot"
            )));
        }
        out.insert(key.as_str(), value.clone());
    }
    Ok(Bson::Document(out))
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
                return Err(Error::InvalidQuery(format!(
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
// Type conversion
// ---------------------------------------------------------------------------

/// Convert a non-null value, or say why it cannot be.
///
/// The pairs follow MongoDB's table: a number is exact where it can be and an
/// error where it would be truncated *in range* (`$toInt` of 2^40) rather than
/// silently wrapped; a string is parsed strictly, so `"12abc"` is an error and
/// not 12; a date is its epoch milliseconds in either direction. What is not
/// in the table — a document to a number, an array to a date — is an error
/// naming both types, which `onError` turns into a value.
fn convert(value: &Bson, to: ConvertTo) -> Result<Bson> {
    let unsupported = || {
        Error::InvalidQuery(format!(
            "$convert cannot convert {} to {}",
            type_name(value),
            to.name()
        ))
    };
    match to {
        ConvertTo::Double => Ok(Bson::Double(match value {
            Bson::Double(d) => *d,
            Bson::Int32(n) => f64::from(*n),
            Bson::Int64(n) => *n as f64,
            Bson::Boolean(b) => f64::from(u8::from(*b)),
            Bson::DateTime(dt) => dt.timestamp_millis() as f64,
            Bson::String(s) => s.parse::<f64>().map_err(|_| {
                Error::InvalidQuery(format!("$convert cannot read {s:?} as a double"))
            })?,
            _ => return Err(unsupported()),
        })),
        ConvertTo::Int => {
            let n = convert_integer(value, to)?;
            i32::try_from(n).map(Bson::Int32).map_err(|_| {
                Error::InvalidQuery(format!("$convert: {n} is out of range for an int"))
            })
        }
        ConvertTo::Long => convert_integer(value, to).map(Bson::Int64),
        ConvertTo::String => Ok(Bson::String(match value {
            Bson::String(s) => s.clone(),
            Bson::Int32(n) => n.to_string(),
            Bson::Int64(n) => n.to_string(),
            Bson::Double(d) => double_to_string(*d),
            Bson::Boolean(b) => b.to_string(),
            // ISO 8601 with milliseconds, which is `$dateToString`'s default
            // and what `$toDate` reads back.
            Bson::DateTime(dt) => format_date(dt.timestamp_millis(), "%Y-%m-%dT%H:%M:%S.%LZ")?,
            Bson::ObjectId(oid) => oid.to_hex(),
            _ => return Err(unsupported()),
        })),
        // Everything present is true except a zero, and `"false"` is a
        // non-empty string and therefore true. The rule is `truthy`'s alone;
        // a null or an undefined never reaches here, because `$convert`
        // answers it with `onNull` first.
        ConvertTo::Bool => Ok(Bson::Boolean(truthy(value))),
        ConvertTo::Date => Ok(Bson::DateTime(match value {
            Bson::DateTime(dt) => *dt,
            Bson::Int32(n) => bson::DateTime::from_millis(i64::from(*n)),
            Bson::Int64(n) => bson::DateTime::from_millis(*n),
            Bson::Double(d) => bson::DateTime::from_millis(double_to_i64(*d).ok_or_else(|| {
                Error::InvalidQuery(format!("$convert: {d} is not a whole number of milliseconds"))
            })?),
            Bson::String(s) => parse_date(s)?,
            // The creation time an ObjectId carries in its leading bytes.
            Bson::ObjectId(oid) => oid.timestamp(),
            _ => return Err(unsupported()),
        })),
        ConvertTo::ObjectId => match value {
            Bson::ObjectId(_) => Ok(value.clone()),
            Bson::String(s) => bson::oid::ObjectId::parse_str(s).map(Bson::ObjectId).map_err(|_| {
                Error::InvalidQuery(format!(
                    "$convert cannot read {s:?} as an ObjectId; it takes 24 hexadecimal characters"
                ))
            }),
            _ => Err(unsupported()),
        },
    }
}

/// The integer a value converts to, before the target's range is applied.
fn convert_integer(value: &Bson, to: ConvertTo) -> Result<i64> {
    match value {
        Bson::Int32(n) => Ok(i64::from(*n)),
        Bson::Int64(n) => Ok(*n),
        Bson::Boolean(b) => Ok(i64::from(*b)),
        // Truncated toward zero, as MongoDB does; a value with no integer
        // representation at all — NaN, infinity, beyond 2^63 — is an error.
        Bson::Double(d) => double_to_i64(*d).ok_or_else(|| {
            Error::InvalidQuery(format!("$convert: {d} has no {} representation", to.name()))
        }),
        // A base-10 integer and nothing else: `"1.5"` is not one, and neither
        // is `"12abc"`. Reading a prefix would turn a data error into a number.
        Bson::String(s) => s.parse::<i64>().map_err(|_| {
            Error::InvalidQuery(format!("$convert cannot read {s:?} as {}", article(to.name())))
        }),
        // Epoch milliseconds fit a long and never an int, so only the one
        // direction is offered rather than an int conversion that always
        // fails on range.
        Bson::DateTime(dt) if to == ConvertTo::Long => Ok(dt.timestamp_millis()),
        _ => Err(Error::InvalidQuery(format!(
            "$convert cannot convert {} to {}",
            type_name(value),
            to.name()
        ))),
    }
}

/// A double truncated toward zero, when that is an `i64`.
fn double_to_i64(d: f64) -> Option<i64> {
    // `as` saturates, so the range is checked first: 2^63 is exactly
    // representable as a double and is one past the largest i64.
    let t = d.trunc();
    (d.is_finite() && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&t))
        .then_some(t as i64)
}

/// Rust's shortest round-trip rendering, with the non-finite spellings
/// MongoDB uses. `2.0` prints as `2`, exactly as MongoDB's `$toString` does.
fn double_to_string(d: f64) -> String {
    if d.is_nan() {
        "NaN".to_string()
    } else if d.is_infinite() {
        if d > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        d.to_string()
    }
}

/// RFC 3339 first, then the looser spellings MongoDB accepts: a bare date, a
/// space between date and time, and a missing zone — read as UTC, because BSON
/// dates carry no zone and there is no other honest choice.
fn parse_date(s: &str) -> Result<bson::DateTime> {
    if let Ok(dt) = bson::DateTime::parse_rfc3339_str(s) {
        return Ok(dt);
    }
    let mut candidate = s.trim().replacen(' ', "T", 1);
    let time_at = match candidate.find('T') {
        Some(i) => i,
        None => {
            candidate.push_str("T00:00:00");
            candidate.len() - 9
        }
    };
    let has_zone = candidate.ends_with(['Z', 'z'])
        || candidate[time_at..].contains('+')
        || candidate[time_at..].contains('-');
    if !has_zone {
        candidate.push('Z');
    }
    bson::DateTime::parse_rfc3339_str(&candidate).map_err(|_| {
        Error::InvalidQuery(format!(
            "$convert cannot read {s:?} as a date; use RFC 3339, e.g. \"2026-08-12T13:45:07Z\""
        ))
    })
}

fn article(noun: &str) -> String {
    match noun.chars().next() {
        Some('a' | 'e' | 'i' | 'o' | 'u') => format!("an {noun}"),
        _ => format!("a {noun}"),
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// MongoDB's truthiness: `false`, `null`, missing and zero are false, and
/// **everything else** — including the empty string and the empty array — is
/// true.
///
/// Zero is zero of every numeric type: a stored `Decimal128` zero, of either
/// sign and any exponent (`0`, `-0`, `0.000`, `0E-6176`), is false like `0`
/// and `0.0`. A `NaN` or an infinity is not zero, so it is true, for a double
/// and a `Decimal128` alike.
pub fn truthy(value: &Bson) -> bool {
    match value {
        Bson::Boolean(b) => *b,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(0) | Bson::Int64(0) => false,
        Bson::Double(d) => *d != 0.0,
        Bson::Decimal128(d) => !decimal128_is_zero(d),
        _ => true,
    }
}

/// Whether a `Decimal128` holds a zero, read from its IEEE 754-2008 BID
/// encoding rather than its text.
///
/// The value is zero exactly when it is finite and its coefficient is zero;
/// the sign and the exponent do not matter. A coefficient above
/// `10^34 - 1` is non-canonical, and the standard reads it as zero, as the
/// `bson` crate's own decoding does. That covers every encoding whose two
/// bits after the sign are `11` and that is not a `NaN` or an infinity: its
/// coefficient starts at `2^113`, which is already above the limit.
pub(crate) fn decimal128_is_zero(d: &bson::Decimal128) -> bool {
    const MAX_COEFFICIENT: u128 = 9_999_999_999_999_999_999_999_999_999_999_999;
    // BSON stores the 128 bits little-endian; bit 127 is the sign.
    let bits = u128::from_le_bytes(d.bytes());
    if (bits >> 122) & 0b1_1110 == 0b1_1110 {
        return false; // `11110` is an infinity, `11111` a NaN
    }
    if (bits >> 125) & 0b11 == 0b11 {
        return true; // a non-canonical coefficient, read as zero
    }
    let coefficient = bits & ((1u128 << 113) - 1);
    coefficient == 0 || coefficient > MAX_COEFFICIENT
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
        Bson::ObjectId(_) => "an ObjectId",
        Bson::Decimal128(_) => "a Decimal128",
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

    // -- field paths through arrays ------------------------------------------

    fn field(path: &str, d: Document) -> Bson {
        on(Bson::String(path.into()), d)
    }

    fn strings(items: &[&str]) -> Bson {
        Bson::Array(items.iter().map(|s| Bson::String((*s).into())).collect())
    }

    #[test]
    fn a_path_through_an_array_is_every_element_value() {
        let d = doc! {"items": [{"sku": "a"}, {"sku": "b"}]};
        assert_eq!(field("$items.sku", d), strings(&["a", "b"]));
    }

    #[test]
    fn a_path_that_crosses_no_array_is_the_single_value() {
        let d = doc! {"a": {"b": {"c": 7}}, "n": 1};
        assert_eq!(field("$a.b.c", d.clone()), Bson::Int32(7));
        assert_eq!(field("$n", d), Bson::Int32(1));
    }

    #[test]
    fn a_trailing_array_is_returned_as_it_is() {
        let d = doc! {"tags": ["x", "y"], "a": [{"b": [1, 2]}]};
        assert_eq!(field("$tags", d.clone()), strings(&["x", "y"]));
        // The last segment lands on an array inside a crossed one: that array
        // is one element of the result, not spliced into it.
        assert_eq!(
            field("$a.b", d),
            Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])])
        );
    }

    #[test]
    fn each_array_crossed_adds_one_level_and_no_more() {
        let d = doc! {"a": [{"b": [1, 2]}, {"b": 3}]};
        assert_eq!(
            field("$a.b", d),
            Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3)])
        );
        let d = doc! {"a": [{"b": [{"c": 1}, {"c": 2}]}]};
        assert_eq!(
            field("$a.b.c", d),
            Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])])
        );
        // Two documents each crossing an inner array: one inner array each.
        let d = doc! {"a": [{"b": [{"c": 1}]}, {"b": [{"c": 2}, {"c": 3}]}]};
        assert_eq!(
            field("$a.b.c", d),
            Bson::Array(vec![
                Bson::Array(vec![Bson::Int32(1)]),
                Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)]),
            ])
        );
    }

    #[test]
    fn free_variables_are_the_names_nothing_inside_binds() {
        let free = |v: Bson| Expr::free_variables(&v).unwrap();
        assert_eq!(free(doc! { "$gt": ["$$line.qty", "$$line.min"] }.into()), ["line"]);
        assert_eq!(free(doc! { "$gt": ["$$b.x", "$$a.y"] }.into()), ["a", "b"]);
        assert!(free(doc! { "$gt": ["$qty", 1] }.into()).is_empty());
        assert!(free(Bson::Boolean(true)).is_empty());
        // `$$ROOT` is a system name, not a user one.
        assert!(free(doc! { "$eq": ["$$ROOT.a", 1] }.into()).is_empty());
        // Bound inside: by `$let`, `$filter`, `$map` and `$reduce`.
        assert_eq!(
            free(doc! { "$let": { "vars": { "m": "$$line.min" }, "in": { "$gt": ["$$line.qty", "$$m"] } } }.into()),
            ["line"]
        );
        assert_eq!(
            free(doc! { "$size": { "$filter": { "input": "$$line.tags", "as": "t", "cond": { "$eq": ["$$t", "x"] } } } }.into()),
            ["line"]
        );
        assert!(
            free(
                doc! { "$map": { "input": [1, 2], "as": "n", "in": { "$add": ["$$n", 1] } } }
                    .into()
            )
            .is_empty()
        );
        assert!(
            free(doc! { "$reduce": { "input": [1, 2], "initialValue": 0, "in": { "$add": ["$$value", "$$this"] } } }.into())
                .is_empty()
        );
        // A name bound in one place and free in another is free.
        assert_eq!(
            free(
                doc! { "$add": [
                    { "$map": { "input": [1], "as": "n", "in": "$$n" } },
                    "$$n",
                ] }
                .into()
            ),
            ["n"]
        );
        // A string a `$literal` holds is not a reference.
        assert!(free(doc! { "$eq": ["$a", { "$literal": "$$nope" }] }.into()).is_empty());
        // An expression that does not parse is that error.
        assert!(Expr::free_variables(&doc! { "$nope": 1 }.into()).is_err());
    }

    /// Asking whether a name is bound is one probe, however many names are in
    /// scope: a `$let` with fifty thousand variables, each read once, is fifty
    /// thousand probes, where a walk of the scope for every reference was
    /// quadratic and held a request worker for seconds. Counted, not timed.
    #[test]
    fn a_scope_lookup_is_one_probe_per_reference_however_many_names_are_bound() {
        for k in [10usize, 50_000] {
            let mut vars = Document::new();
            for i in 0..k {
                vars.insert(format!("v{i}"), 1);
            }
            let names: Vec<Bson> = (0..k).map(|i| Bson::String(format!("$$v{i}"))).collect();
            let entry = Bson::Document(doc! { "$let": { "vars": vars, "in": { "$add": names } } });
            let before = LOOKUPS.with(std::cell::Cell::get);
            let parsed = Expr::parse(&entry).unwrap();
            assert_eq!(
                LOOKUPS.with(std::cell::Cell::get) - before,
                k,
                "{k} references, one probe each"
            );
            // And it still means what it said.
            assert_eq!(parsed.eval(&doc! {}).unwrap(), Bson::Int64(k as i64));
        }
        // A name nothing binds is one probe too, and refused.
        let before = LOOKUPS.with(std::cell::Cell::get);
        assert!(Expr::parse(&Bson::String("$$nope".into())).is_err());
        assert_eq!(LOOKUPS.with(std::cell::Cell::get) - before, 1);
        // A system name is not looked up at all.
        let before = LOOKUPS.with(std::cell::Cell::get);
        Expr::parse(&Bson::String("$$ROOT".into())).unwrap();
        assert_eq!(LOOKUPS.with(std::cell::Cell::get) - before, 0);
    }

    /// Reading a `$let`'s variables is a bisect past a handful of them, not a
    /// walk: fifty thousand variables, each read once, compare names about
    /// fifty thousand times seventeen, where a walk compared about a billion.
    /// Counted, not timed, and the answers are the same as a small `$let`'s.
    #[test]
    fn reading_a_let_with_thousands_of_variables_bisects_its_frame() {
        let k = 50_000usize;
        let mut vars = Document::new();
        for i in 0..k {
            vars.insert(format!("v{i}"), i as i64);
        }
        let names: Vec<Bson> = (0..k).map(|i| Bson::String(format!("$$v{i}"))).collect();
        let big = Expr::parse(&Bson::Document(
            doc! { "$let": { "vars": vars, "in": { "$add": names } } },
        ))
        .unwrap();
        let before = FRAME_COMPARISONS.with(std::cell::Cell::get);
        let sum = big.eval(&doc! {}).unwrap();
        let compared = FRAME_COMPARISONS.with(std::cell::Cell::get) - before;
        assert_eq!(sum, Bson::Int64((k as i64) * (k as i64 - 1) / 2));
        assert!(compared <= k * 20, "{compared} name comparisons to read {k} variables once each");
        // The shadowing and the parent chain of a big frame are a small one's:
        // the inner `$let` is small and rebinds a name the big one holds.
        let mut vars = Document::new();
        for i in 0..40 {
            vars.insert(format!("v{i}"), i);
        }
        let shadow = Expr::parse(&Bson::Document(doc! { "$let": { "vars": vars, "in": {
        "$add": [ "$$v7", { "$let": { "vars": { "v7": 100 }, "in": "$$v7" } }, "$$v39" ] } } }))
        .unwrap();
        assert_eq!(shadow.eval(&doc! {}).unwrap(), Bson::Int64(7 + 100 + 39));
        // A name only an enclosing frame binds is found through a big one.
        let mut vars = Document::new();
        for i in 0..40 {
            vars.insert(format!("w{i}"), i);
        }
        let through =
            Expr::parse(&Bson::Document(doc! { "$let": { "vars": { "outer": 5 }, "in": {
            "$let": { "vars": vars, "in": { "$add": ["$$outer", "$$w3"] } } } } }))
            .unwrap();
        assert_eq!(through.eval(&doc! {}).unwrap(), Bson::Int64(8));
    }

    /// The frame size at which a `$let`'s names stop being scanned and are
    /// bisected, held at both edges: sixteen names are walked, so the first one
    /// defined is the sixteenth compared, and seventeen are bisected. Counted,
    /// and every variable is read so a wrong answer shows.
    #[test]
    fn a_frame_is_bisected_from_seventeen_names_and_not_before() {
        let read_first = |n: usize| -> usize {
            let mut vars = Document::new();
            for i in 0..n {
                vars.insert(format!("v{i}"), i as i64);
            }
            let names: Vec<Bson> = (0..n).map(|i| Bson::String(format!("$$v{i}"))).collect();
            let all = Expr::parse(&Bson::Document(
                doc! { "$let": { "vars": vars.clone(), "in": { "$add": names } } },
            ))
            .unwrap();
            assert_eq!(all.eval(&doc! {}).unwrap(), Bson::Int64((n * (n - 1) / 2) as i64));
            let first =
                Expr::parse(&Bson::Document(doc! { "$let": { "vars": vars, "in": "$$v0" } }))
                    .unwrap();
            let before = FRAME_COMPARISONS.with(std::cell::Cell::get);
            assert_eq!(first.eval(&doc! {}).unwrap(), Bson::Int64(0));
            FRAME_COMPARISONS.with(std::cell::Cell::get) - before
        };
        assert_eq!(read_first(16), 16, "sixteen names are scanned");
        let at_seventeen = read_first(17);
        assert!((1..=8).contains(&at_seventeen), "{at_seventeen} comparisons at 17 names");
    }

    /// Shadowing and leaving a scope, which the index must keep the way the
    /// walk did: an inner binding of a name leaves the outer one bound, and a
    /// name is unbound once the last construct that bound it is left.
    #[test]
    fn a_scope_index_keeps_shadowing_and_leaving_a_scope() {
        let mut declared = Declared::new(&["a".to_string(), "a".to_string()]);
        assert!(declared.binds("a") && !declared.binds("b"));
        let depth = declared.depth();
        declared.push("b");
        declared.push("a");
        assert!(declared.binds("b"));
        declared.truncate(depth);
        assert!(declared.binds("a"), "the outer bindings of `a` are still in scope");
        assert!(!declared.binds("b"), "`b` went with its scope");
        declared.truncate(0);
        assert!(!declared.binds("a"));

        // Through the parser: an inner `$let` of `x` shadows and then restores,
        // and a name is unbound after the construct that bound it.
        let nested = doc! { "$let": { "vars": { "x": 1 }, "in": { "$add": [
            { "$let": { "vars": { "x": 2 }, "in": "$$x" } },
            "$$x",
        ] } } };
        assert_eq!(Expr::parse(&nested.into()).unwrap().eval(&doc! {}).unwrap(), Bson::Int64(3));
        let leaked = doc! { "$add": [
            { "$let": { "vars": { "x": 2 }, "in": "$$x" } },
            "$$x",
        ] };
        assert!(Expr::parse(&leaked.into()).is_err(), "`x` is not bound outside its `$let`");
        let mapped = doc! { "$map": { "input": [1, 2], "as": "n", "in": { "$add": ["$$n", 1] } } };
        assert!(Expr::parse(&mapped.into()).is_ok());
        assert!(
            Expr::parse(
                &doc! { "$add": [{ "$map": { "input": [1], "as": "n", "in": "$$n" } }, "$$n"] }
                    .into()
            )
            .is_err(),
            "`n` is not bound outside its `$map`"
        );
    }

    /// Finding the free names is one parse, however many names the expression
    /// holds, bound or not: an earlier build parsed once per candidate name, and
    /// a `$let` with thousands of variables took minutes. Counted, not timed.
    #[test]
    fn free_variables_costs_one_parse_however_many_names_it_holds() {
        for k in [10usize, 2000] {
            let mut vars = Document::new();
            for i in 0..k {
                vars.insert(format!("v{i}"), i as i32);
            }
            let mut names: Vec<Bson> = (0..k).map(|i| Bson::String(format!("$$v{i}"))).collect();
            names.push(Bson::String("$$line.x".into()));
            let bound = Bson::Document(doc! { "$let": { "vars": vars, "in": { "$add": names } } });
            let before = PARSES.with(std::cell::Cell::get);
            assert_eq!(Expr::free_variables(&bound).unwrap(), ["line"], "{k} bound names");
            assert_eq!(PARSES.with(std::cell::Cell::get) - before, 1, "{k} bound names");

            // The same count of names, all of them free.
            let free: Vec<Bson> = (0..k).map(|i| Bson::String(format!("$$w{i}"))).collect();
            let before = PARSES.with(std::cell::Cell::get);
            let found = Expr::free_variables(&Bson::Document(doc! { "$add": free })).unwrap();
            assert_eq!(found.len(), k, "{k} free names");
            assert_eq!(PARSES.with(std::cell::Cell::get) - before, 1, "{k} free names");
        }
        // The collecting mode ends with the call: a plain parse refuses again.
        assert!(Expr::parse(&Bson::String("$$nope".into())).is_err());
    }

    #[test]
    fn an_expression_reads_the_document_when_a_field_or_a_system_name_is_in_it() {
        let reads =
            |v: Bson| Expr::parse_with_vars(&v, &["line".to_string()]).unwrap().reads_document();
        assert!(!reads(doc! { "$gt": ["$$line.qty", 5] }.into()));
        assert!(!reads(Bson::Int32(1)));
        assert!(reads(doc! { "$gt": ["$$line.qty", "$min"] }.into()));
        assert!(reads(doc! { "$eq": ["$$line", "$$ROOT"] }.into()));
        assert!(reads(doc! { "$eq": ["$$CURRENT.a", 1] }.into()));
        // Under each construct that holds an expression.
        for deep in [
            doc! { "$cond": [true, "$a", 0] },
            doc! { "$switch": { "branches": [ { "case": true, "then": "$a" } ], "default": 0 } },
            doc! { "$switch": { "branches": [ { "case": true, "then": 1 } ], "default": "$a" } },
            doc! { "$switch": { "branches": [ { "case": "$a", "then": 1 } ] } },
            doc! { "$dateToString": { "date": "$d", "format": "%Y" } },
            doc! { "$convert": { "input": "$a", "to": "int" } },
            doc! { "$convert": { "input": 1, "to": "int", "onError": "$a" } },
            doc! { "$convert": { "input": 1, "to": "int", "onNull": "$a" } },
            doc! { "$let": { "vars": { "x": "$a" }, "in": "$$x" } },
            doc! { "$let": { "vars": { "x": 1 }, "in": "$a" } },
            doc! { "$filter": { "input": "$a", "as": "t", "cond": true } },
            doc! { "$filter": { "input": [1], "as": "t", "cond": "$a" } },
            doc! { "$filter": { "input": [1], "as": "t", "cond": true, "limit": "$a" } },
            doc! { "$map": { "input": "$a", "as": "t", "in": 1 } },
            doc! { "$map": { "input": [1], "as": "t", "in": "$a" } },
            doc! { "$reduce": { "input": "$a", "initialValue": 0, "in": 1 } },
            doc! { "$reduce": { "input": [1], "initialValue": "$a", "in": 1 } },
            doc! { "$reduce": { "input": [1], "initialValue": 0, "in": "$a" } },
            doc! { "x": "$a" },
        ] {
            assert!(reads(deep.clone().into()), "{deep}");
        }
    }

    #[test]
    fn elements_missing_the_field_are_skipped_not_null_filled() {
        let d = doc! {"items": [{"sku": "a"}, {"qty": 1}, {"sku": "c"}]};
        assert_eq!(field("$items.sku", d), strings(&["a", "c"]));
        // Nothing found is an empty array, not null: the array was crossed.
        let d = doc! {"items": [{"qty": 1}, {"qty": 2}]};
        assert_eq!(field("$items.sku", d), Bson::Array(vec![]));
        assert_eq!(field("$items.sku", doc! {"items": []}), Bson::Array(vec![]));
    }

    #[test]
    fn elements_that_are_not_documents_are_skipped() {
        let d =
            doc! {"items": [{"sku": "a"}, 7, "b", Bson::Null, [{"sku": "nested"}], {"sku": "z"}]};
        // The nested array is not a document either: a path does not descend
        // into an array directly inside an array.
        assert_eq!(field("$items.sku", d), strings(&["a", "z"]));
    }

    #[test]
    fn a_numeric_segment_is_a_field_name_not_an_index() {
        let d = doc! {"items": [{"sku": "a"}, {"sku": "b"}]};
        // No element has a field called "0", so the fan-out finds nothing;
        // `$arrayElemAt` is how an element is addressed by position.
        assert_eq!(field("$items.0.sku", d.clone()), Bson::Array(vec![]));
        assert_eq!(field("$items.0", d), Bson::Array(vec![]));
        // But an element that really has a field named "0" is read.
        let d = doc! {"items": [{"0": {"sku": "zero"}}]};
        assert_eq!(field("$items.0.sku", d), strings(&["zero"]));
        // And on a document, "0" is an ordinary field name.
        assert_eq!(field("$a.0", doc! {"a": {"0": 5}}), Bson::Int32(5));
    }

    #[test]
    fn a_missing_path_is_still_null_whether_or_not_it_starts_in_an_array() {
        let d = doc! {"a": 1, "items": [{"sku": "a"}]};
        assert_eq!(field("$nope", d.clone()), Bson::Null);
        assert_eq!(field("$nope.sku", d.clone()), Bson::Null);
        // A segment landing on a scalar is missing, as before.
        assert_eq!(field("$a.b", d.clone()), Bson::Null);
        // An explicit null is a value, not absence.
        assert_eq!(field("$n", doc! {"n": Bson::Null}), Bson::Null);
        assert_eq!(field("$items.sku.x", d), Bson::Array(vec![]));
    }

    #[test]
    fn size_counts_a_fanned_path() {
        let d = doc! {"items": [{"sku": "a"}, {"sku": "b"}]};
        assert_eq!(on(doc! {"$size": "$items.sku"}.into(), d), Bson::Int64(2));
    }

    #[test]
    fn root_and_current_paths_fan_out_like_a_field_path() {
        let d = doc! {"items": [{"sku": "a"}, {"sku": "b"}]};
        assert_eq!(field("$$ROOT.items.sku", d.clone()), strings(&["a", "b"]));
        assert_eq!(field("$$CURRENT.items.sku", d), strings(&["a", "b"]));
    }

    #[test]
    fn a_variable_path_fans_out_over_what_the_variable_holds() {
        // Inside `$map`, `$$i` is one element and `$$i.tags.name` crosses the
        // element's own array.
        let d = doc! {"items": [
            {"tags": [{"name": "x"}, {"name": "y"}]},
            {"tags": [{"name": "z"}]},
            {"tags": []},
        ]};
        let expr = doc! {"$map": {"input": "$items", "as": "i", "in": "$$i.tags.name"}};
        assert_eq!(
            on(expr.into(), d.clone()),
            Bson::Array(vec![strings(&["x", "y"]), strings(&["z"]), Bson::Array(vec![])])
        );
        // A `$let` that bound the whole array: the path fans out over it as
        // `$items.tags.name` would.
        let expr = doc! {"$let": {"vars": {"rows": "$items"}, "in": "$$rows.tags.name"}};
        assert_eq!(
            on(expr.into(), d.clone()),
            Bson::Array(vec![strings(&["x", "y"]), strings(&["z"]), Bson::Array(vec![])])
        );
        // The same variable bound to the fanned path itself.
        let expr =
            doc! {"$let": {"vars": {"names": "$items.tags.name"}, "in": {"$size": "$$names"}}};
        assert_eq!(on(expr.into(), d), Bson::Int64(3));
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
    fn an_unknown_variable_is_refused_at_parse_rather_than_read_as_a_field() {
        // Parsing `$$this` outside anything that binds it as a field named
        // `$this` would silently yield null in every row.
        let err = Expr::parse(&Bson::String("$$this".into())).unwrap_err();
        assert!(matches!(err, Error::InvalidQuery(_)), "{err:?}");
        assert!(err.to_string().contains("$$this"), "{err}");
        // And so would a typo inside a construct that binds something else.
        let expr = doc! {"$map": {"input": "$xs", "as": "x", "in": "$$y"}};
        assert!(Expr::parse(&expr.into()).is_err());
    }

    #[test]
    fn an_unsupported_system_variable_says_so() {
        // `$$NOW` is a feature this does not have, not a typo, and the error
        // class says which.
        let err = Expr::parse(&Bson::String("$$NOW".into())).unwrap_err();
        assert!(matches!(err, Error::UnsupportedOperator { .. }), "{err:?}");
        assert!(Expr::parse(&Bson::String("$$".into())).is_err());
        assert!(Expr::parse(&Bson::String("$$ROOT.".into())).is_err());
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

    #[test]
    fn switch_refuses_an_unknown_key_at_the_top_level_and_in_a_branch() {
        // Same closure `$filter`/`$map`/`$reduce`/`$let` already have via
        // `named_spec`, and the same hazard finding 11 named: an unrecognized
        // key silently ignored is a typo that quietly changes the result.
        let err = Expr::parse(
            &doc! {"$switch": {"branches": [{"case": true, "then": 1}], "bogus": 1}}.into(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("bogus"), "{err}");

        let err = Expr::parse(
            &doc! {"$switch": {"branches": [{"case": true, "then": 1, "bogus": 1}]}}.into(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("bogus"), "{err}");
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
    fn date_to_string_refuses_an_unknown_key() {
        // `{"formt": "%Y"}` — the finding's own shape (11), moved to a
        // second operand this codebase already had: before this fix it
        // silently kept the default ISO-8601 format in every row rather than
        // naming the typo.
        let err = Expr::parse(&doc! {"$dateToString": {"date": "$t", "formt": "%Y"}}.into())
            .unwrap_err()
            .to_string();
        assert!(err.contains("formt"), "{err}");
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

    // -- variables --------------------------------------------------------

    #[test]
    fn root_is_the_whole_document_and_current_is_its_alias() {
        let d = doc! {"a": 1, "b": {"c": 2}};
        assert_eq!(on(Bson::String("$$ROOT".into()), d.clone()), Bson::Document(d.clone()));
        assert_eq!(on(Bson::String("$$CURRENT".into()), d.clone()), Bson::Document(d.clone()));
        // Inside an object expression, so a `$project` can embed the source.
        assert_eq!(
            on(doc! {"src": "$$ROOT", "n": "$a"}.into(), d.clone()),
            Bson::Document(doc! {"src": d, "n": 1})
        );
    }

    #[test]
    fn a_variable_path_reads_into_its_value() {
        let d = doc! {"a": 1, "b": {"c": 2}};
        assert_eq!(on(Bson::String("$$ROOT.b.c".into()), d.clone()), Bson::Int32(2));
        assert_eq!(on(Bson::String("$$ROOT.nope".into()), d), Bson::Null);
        // On a bound variable holding a document.
        let expr = doc! {"$let": {"vars": {"p": {"x": 5}}, "in": "$$p.x"}};
        assert_eq!(ok(expr.into()), Bson::Int32(5));
        // A path into a scalar is a missing field, exactly as `$a.b` is when
        // `a` is a number.
        let expr = doc! {"$let": {"vars": {"p": 5}, "in": "$$p.x"}};
        assert_eq!(ok(expr.into()), Bson::Null);
    }

    #[test]
    fn let_binds_names_for_its_body() {
        let expr = doc! {"$let": {
            "vars": {"total": {"$multiply": ["$qty", "$price"]}, "tax": 0.2},
            "in": {"$multiply": ["$$total", {"$add": [1, "$$tax"]}]},
        }};
        assert_eq!(on(expr.into(), doc! {"qty": 10, "price": 5}), Bson::Double(60.0));
    }

    #[test]
    fn let_values_see_the_enclosing_scope_not_each_other() {
        // `b` cannot read `a`: the values are evaluated together in the outer
        // scope, and the parser says so rather than the evaluator yielding null.
        let expr = doc! {"$let": {"vars": {"a": 1, "b": "$$a"}, "in": "$$b"}};
        assert!(Expr::parse(&expr.into()).is_err());
        // But an outer `$let` is visible from an inner one's values.
        let expr = doc! {"$let": {"vars": {"a": 1}, "in": {
            "$let": {"vars": {"b": {"$add": ["$$a", 1]}}, "in": "$$b"}
        }}};
        assert_eq!(ok(expr.into()), Bson::Int64(2));
    }

    #[test]
    fn an_inner_binding_shadows_an_outer_one() {
        let expr = doc! {"$let": {"vars": {"x": "outer"}, "in": {
            "$let": {"vars": {"x": "inner"}, "in": "$$x"}
        }}};
        assert_eq!(ok(expr.into()), Bson::String("inner".into()));
        // And the outer one is back once the inner construct closes.
        let expr = doc! {"$let": {"vars": {"x": "outer"}, "in": {
            "$concat": [{"$let": {"vars": {"x": "inner"}, "in": "$$x"}}, "-", "$$x"]
        }}};
        assert_eq!(ok(expr.into()), Bson::String("inner-outer".into()));
    }

    #[test]
    fn a_variable_name_follows_mongos_rule() {
        assert!(Expr::parse(&doc! {"$let": {"vars": {"Total": 1}, "in": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": {"vars": {"a-b": 1}, "in": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": {"vars": {"": 1}, "in": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": {"vars": {"a_1": 1}, "in": "$$a_1"}}.into()).is_ok());
        assert!(Expr::parse(&doc! {"$map": {"input": [], "as": "X", "in": 1}}.into()).is_err());
    }

    #[test]
    fn let_needs_its_two_parts_and_nothing_else() {
        assert!(Expr::parse(&doc! {"$let": {"vars": {"a": 1}}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": {"in": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": {"vars": [], "in": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": {"vars": {}, "in": 1, "extra": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$let": "$x"}.into()).is_err());
    }

    #[test]
    fn a_variable_bound_by_the_caller_is_visible_to_the_expression() {
        // The `$lookup` `let` path: parsed with the name declared, evaluated
        // with it bound.
        let expr = Expr::parse_with_vars(
            &doc! {"$add": ["$$order_qty", "$qty"]}.into(),
            &["order_qty".to_string()],
        )
        .unwrap();
        let bound = Bson::Int32(40);
        let frame = [("order_qty", &bound)];
        let d = doc! {"qty": 2};
        assert_eq!(expr.eval_in(&Scope::with_bindings(&d, &frame)).unwrap(), Bson::Int64(42));
        // Evaluating with fewer bindings than it was parsed with is an error,
        // not a null.
        assert!(expr.eval(&d).is_err());
    }

    // -- arrays: the positional operators ---------------------------------

    fn arr(items: Vec<i32>) -> Bson {
        Bson::Array(items.into_iter().map(Bson::Int32).collect())
    }

    #[test]
    fn size_counts_elements_and_refuses_a_non_array() {
        assert_eq!(on(doc! {"$size": "$xs"}.into(), doc! {"xs": [1, 2, 3]}), Bson::Int64(3));
        assert_eq!(on(doc! {"$size": "$xs"}.into(), doc! {"xs": []}), Bson::Int64(0));
        assert_eq!(on(doc! {"$size": "$gone"}.into(), doc! {}), Bson::Null);
        assert!(ev(doc! {"$size": "text"}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$size": 5}.into(), &Document::new()).is_err());
    }

    #[test]
    fn array_elem_at_counts_from_either_end_and_is_null_past_them() {
        let d = doc! {"xs": ["a", "b", "c"]};
        assert_eq!(on(doc! {"$arrayElemAt": ["$xs", 0]}.into(), d.clone()), "a".into());
        assert_eq!(on(doc! {"$arrayElemAt": ["$xs", -1]}.into(), d.clone()), "c".into());
        assert_eq!(on(doc! {"$arrayElemAt": ["$xs", 2.0]}.into(), d.clone()), "c".into());
        assert_eq!(on(doc! {"$arrayElemAt": ["$xs", 3]}.into(), d.clone()), Bson::Null);
        assert_eq!(on(doc! {"$arrayElemAt": ["$xs", -4]}.into(), d.clone()), Bson::Null);
        assert_eq!(on(doc! {"$arrayElemAt": ["$gone", 0]}.into(), d.clone()), Bson::Null);
        assert_eq!(on(doc! {"$arrayElemAt": ["$xs", "$gone"]}.into(), d.clone()), Bson::Null);
        // A fractional index is a mistake, not a request for element 1.
        assert!(ev(doc! {"$arrayElemAt": ["$xs", 1.5]}.into(), &d).is_err());
        assert!(ev(doc! {"$arrayElemAt": ["$xs", "1"]}.into(), &d).is_err());
        assert!(ev(doc! {"$arrayElemAt": ["text", 0]}.into(), &d).is_err());
    }

    #[test]
    fn first_and_last_take_the_ends_and_an_empty_array_is_null() {
        let d = doc! {"xs": [1, 2, 3], "empty": []};
        assert_eq!(on(doc! {"$first": "$xs"}.into(), d.clone()), Bson::Int32(1));
        assert_eq!(on(doc! {"$last": "$xs"}.into(), d.clone()), Bson::Int32(3));
        assert_eq!(on(doc! {"$first": "$empty"}.into(), d.clone()), Bson::Null);
        assert_eq!(on(doc! {"$last": "$gone"}.into(), d.clone()), Bson::Null);
        assert!(ev(doc! {"$first": "text"}.into(), &d).is_err());
    }

    #[test]
    fn slice_takes_from_the_front_the_back_or_a_position() {
        let d = doc! {"xs": [1, 2, 3, 4, 5]};
        assert_eq!(on(doc! {"$slice": ["$xs", 2]}.into(), d.clone()), arr(vec![1, 2]));
        assert_eq!(on(doc! {"$slice": ["$xs", -2]}.into(), d.clone()), arr(vec![4, 5]));
        assert_eq!(on(doc! {"$slice": ["$xs", 0]}.into(), d.clone()), arr(vec![]));
        assert_eq!(on(doc! {"$slice": ["$xs", 1, 2]}.into(), d.clone()), arr(vec![2, 3]));
        assert_eq!(on(doc! {"$slice": ["$xs", -2, 1]}.into(), d.clone()), arr(vec![4]));
        // A window past either end is empty, not an error.
        assert_eq!(on(doc! {"$slice": ["$xs", 10]}.into(), d.clone()), arr(vec![1, 2, 3, 4, 5]));
        assert_eq!(on(doc! {"$slice": ["$xs", 10, 2]}.into(), d.clone()), arr(vec![]));
        assert_eq!(on(doc! {"$slice": ["$xs", -10, 2]}.into(), d.clone()), arr(vec![1, 2]));
        assert_eq!(on(doc! {"$slice": ["$gone", 2]}.into(), d.clone()), Bson::Null);
        // With a position the count must be positive: "from the end" is the
        // two-argument form's job.
        assert!(ev(doc! {"$slice": ["$xs", 1, -2]}.into(), &d).is_err());
        assert!(ev(doc! {"$slice": ["$xs", 1, 0]}.into(), &d).is_err());
        assert!(ev(doc! {"$slice": ["text", 1]}.into(), &d).is_err());
        assert!(Expr::parse(&doc! {"$slice": ["$xs"]}.into()).is_err());
        assert!(Expr::parse(&doc! {"$slice": ["$xs", 1, 2, 3]}.into()).is_err());
    }

    #[test]
    fn concat_arrays_joins_and_null_poisons() {
        let d = doc! {"a": [1], "b": [2, 3]};
        assert_eq!(
            on(doc! {"$concatArrays": ["$a", "$b", []]}.into(), d.clone()),
            arr(vec![1, 2, 3])
        );
        assert_eq!(on(doc! {"$concatArrays": ["$a", "$gone"]}.into(), d.clone()), Bson::Null);
        assert!(ev(doc! {"$concatArrays": ["$a", 5]}.into(), &d).is_err());
    }

    #[test]
    fn in_tests_membership_by_the_canonical_order() {
        let d = doc! {"tags": ["a", "b"], "ns": [5]};
        assert_eq!(on(doc! {"$in": ["b", "$tags"]}.into(), d.clone()), Bson::Boolean(true));
        assert_eq!(on(doc! {"$in": ["z", "$tags"]}.into(), d.clone()), Bson::Boolean(false));
        // 5.0 is in [5], as it is to `$eq` and to an index.
        assert_eq!(on(doc! {"$in": [5.0, "$ns"]}.into(), d.clone()), Bson::Boolean(true));
        assert_eq!(on(doc! {"$in": ["a", "$gone"]}.into(), d.clone()), Bson::Null);
        assert!(ev(doc! {"$in": ["a", "abc"]}.into(), &d).is_err());
        assert!(Expr::parse(&doc! {"$in": ["a"]}.into()).is_err());
    }

    #[test]
    fn index_of_array_finds_the_first_match_within_a_window() {
        let d = doc! {"xs": ["a", "b", "a", "c"]};
        assert_eq!(on(doc! {"$indexOfArray": ["$xs", "a"]}.into(), d.clone()), Bson::Int64(0));
        assert_eq!(on(doc! {"$indexOfArray": ["$xs", "a", 1]}.into(), d.clone()), Bson::Int64(2));
        assert_eq!(
            on(doc! {"$indexOfArray": ["$xs", "c", 0, 3]}.into(), d.clone()),
            Bson::Int64(-1)
        );
        assert_eq!(on(doc! {"$indexOfArray": ["$xs", "z"]}.into(), d.clone()), Bson::Int64(-1));
        assert_eq!(on(doc! {"$indexOfArray": ["$xs", "a", 9]}.into(), d.clone()), Bson::Int64(-1));
        assert_eq!(on(doc! {"$indexOfArray": ["$gone", "a"]}.into(), d.clone()), Bson::Null);
        assert!(ev(doc! {"$indexOfArray": ["$xs", "a", -1]}.into(), &d).is_err());
        assert!(ev(doc! {"$indexOfArray": ["text", "a"]}.into(), &d).is_err());
    }

    #[test]
    fn is_array_never_errors() {
        assert_eq!(on(doc! {"$isArray": "$xs"}.into(), doc! {"xs": [1]}), Bson::Boolean(true));
        assert_eq!(on(doc! {"$isArray": "$xs"}.into(), doc! {"xs": "no"}), Bson::Boolean(false));
        assert_eq!(on(doc! {"$isArray": "$gone"}.into(), doc! {}), Bson::Boolean(false));
        assert_eq!(on(doc! {"$isArray": ["$xs"]}.into(), doc! {"xs": []}), Bson::Boolean(true));
    }

    #[test]
    fn reverse_array_reverses_and_passes_null_through() {
        assert_eq!(
            on(doc! {"$reverseArray": "$xs"}.into(), doc! {"xs": [1, 2, 3]}),
            arr(vec![3, 2, 1])
        );
        assert_eq!(on(doc! {"$reverseArray": "$gone"}.into(), doc! {}), Bson::Null);
        assert!(ev(doc! {"$reverseArray": "abc"}.into(), &Document::new()).is_err());
    }

    fn ints(items: Vec<i64>) -> Bson {
        Bson::Array(items.into_iter().map(Bson::Int64).collect())
    }

    #[test]
    fn range_counts_up_down_and_not_past_its_cap() {
        assert_eq!(ok(doc! {"$range": [0, 4]}.into()), ints(vec![0, 1, 2, 3]));
        assert_eq!(ok(doc! {"$range": [0, 10, 3]}.into()), ints(vec![0, 3, 6, 9]));
        assert_eq!(ok(doc! {"$range": [5, 0, -2]}.into()), ints(vec![5, 3, 1]));
        assert_eq!(ok(doc! {"$range": [4, 0]}.into()), ints(vec![]));
        assert_eq!(ok(doc! {"$range": [0, 4, -1]}.into()), ints(vec![]));
        assert_eq!(on(doc! {"$range": [0, "$gone"]}.into(), doc! {}), Bson::Null);
        assert!(ev(doc! {"$range": [0, 4, 0]}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$range": [0, 1.5]}.into(), &Document::new()).is_err());
        // Refused before anything is allocated.
        let err = ev(doc! {"$range": [0, 1_000_000_000]}.into(), &Document::new()).unwrap_err();
        assert!(err.to_string().contains("limit"), "{err}");
        assert!(ev(doc! {"$range": [i64::MIN, i64::MAX]}.into(), &Document::new()).is_err());
    }

    // -- arrays: the binding operators ------------------------------------

    #[test]
    fn filter_keeps_what_the_condition_accepts() {
        let d = doc! {"xs": [1, 5, 10, 15]};
        assert_eq!(
            on(
                doc! {"$filter": {"input": "$xs", "cond": {"$gte": ["$$this", 5]}}}.into(),
                d.clone()
            ),
            arr(vec![5, 10, 15])
        );
        // A named element variable.
        assert_eq!(
            on(
                doc! {"$filter": {"input": "$xs", "as": "n", "cond": {"$lt": ["$$n", 10]}}}.into(),
                d.clone()
            ),
            arr(vec![1, 5])
        );
        // The condition may read the document as well as the element.
        assert_eq!(
            on(
                doc! {"$filter": {"input": "$xs", "cond": {"$gt": ["$$this", "$min"]}}}.into(),
                doc! {"xs": [1, 5, 10], "min": 4}
            ),
            arr(vec![5, 10])
        );
    }

    #[test]
    fn filter_limit_stops_early_and_null_means_no_limit() {
        let d = doc! {"xs": [1, 2, 3, 4]};
        assert_eq!(
            on(doc! {"$filter": {"input": "$xs", "cond": true, "limit": 2}}.into(), d.clone()),
            arr(vec![1, 2])
        );
        assert_eq!(
            on(
                doc! {"$filter": {"input": "$xs", "cond": true, "limit": "$gone"}}.into(),
                d.clone()
            ),
            arr(vec![1, 2, 3, 4])
        );
        assert!(
            ev(doc! {"$filter": {"input": "$xs", "cond": true, "limit": 0}}.into(), &d).is_err()
        );
        assert!(
            ev(doc! {"$filter": {"input": "$xs", "cond": true, "limit": -1}}.into(), &d).is_err()
        );
    }

    #[test]
    fn filter_on_null_is_null_and_on_a_non_array_is_an_error() {
        assert_eq!(
            on(doc! {"$filter": {"input": "$gone", "cond": true}}.into(), doc! {}),
            Bson::Null
        );
        assert!(
            ev(doc! {"$filter": {"input": "text", "cond": true}}.into(), &Document::new()).is_err()
        );
    }

    #[test]
    fn the_binding_operators_check_their_shape_at_parse() {
        // A typo in a key must fail loudly rather than filter nothing.
        assert!(
            Expr::parse(&doc! {"$filter": {"input": "$xs", "condition": true}}.into()).is_err()
        );
        assert!(Expr::parse(&doc! {"$filter": {"cond": true}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$filter": "$xs"}.into()).is_err());
        assert!(
            Expr::parse(&doc! {"$filter": {"input": "$xs", "as": 5, "cond": true}}.into()).is_err()
        );
        assert!(Expr::parse(&doc! {"$map": {"input": "$xs"}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$map": {"input": "$xs", "in": 1, "cond": 1}}.into()).is_err());
        assert!(Expr::parse(&doc! {"$reduce": {"input": "$xs", "in": 1}}.into()).is_err());
        assert!(
            Expr::parse(&doc! {"$reduce": {"input": "$xs", "initialValue": 0}}.into()).is_err()
        );
    }

    #[test]
    fn map_transforms_each_element() {
        let d = doc! {"xs": [1, 2, 3]};
        assert_eq!(
            on(
                doc! {"$map": {"input": "$xs", "in": {"$multiply": ["$$this", 10]}}}.into(),
                d.clone()
            ),
            ints(vec![10, 20, 30])
        );
        assert_eq!(
            on(doc! {"$map": {"input": "$xs", "as": "x", "in": {"v": "$$x"}}}.into(), d.clone()),
            Bson::Array(vec![
                Bson::Document(doc! {"v": 1}),
                Bson::Document(doc! {"v": 2}),
                Bson::Document(doc! {"v": 3}),
            ])
        );
        assert_eq!(on(doc! {"$map": {"input": "$gone", "in": 1}}.into(), doc! {}), Bson::Null);
        assert!(ev(doc! {"$map": {"input": 5, "in": 1}}.into(), &Document::new()).is_err());
    }

    #[test]
    fn map_reads_a_field_of_each_element_through_the_variable() {
        let d = doc! {"items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 3}]};
        assert_eq!(
            on(doc! {"$map": {"input": "$items", "in": "$$this.sku"}}.into(), d),
            Bson::Array(vec!["a".into(), "b".into()])
        );
    }

    #[test]
    fn nested_iteration_shadows_this_and_named_variables_tell_the_levels_apart() {
        // Inner `$$this` is the inner element; the outer one is unreachable
        // by that name — which is what a lexical scope does.
        let expr = doc! {"$map": {"input": "$rows", "in": {
            "$map": {"input": "$$this", "in": {"$multiply": ["$$this", 10]}}
        }}};
        assert_eq!(
            on(expr.into(), doc! {"rows": [[1, 2], [3]]}),
            Bson::Array(vec![ints(vec![10, 20]), ints(vec![30])])
        );

        // With `as` names, both levels are reachable at once.
        let expr = doc! {"$map": {"input": "$groups", "as": "g", "in": {
            "$filter": {
                "input": "$$g.items",
                "as": "it",
                "cond": {"$gte": ["$$it", "$$g.min"]},
            }
        }}};
        let d = doc! {"groups": [
            {"min": 2, "items": [1, 2, 3]},
            {"min": 10, "items": [5, 10]},
        ]};
        assert_eq!(on(expr.into(), d), Bson::Array(vec![arr(vec![2, 3]), arr(vec![10])]));
    }

    #[test]
    fn reduce_folds_with_value_and_this() {
        let d = doc! {"xs": [1, 2, 3, 4]};
        assert_eq!(
            on(
                doc! {"$reduce": {"input": "$xs", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}}
                    .into(),
                d.clone()
            ),
            Bson::Int64(10)
        );
        // Strings, to show the accumulator is any value.
        assert_eq!(
            on(
                doc! {"$reduce": {"input": "$ws", "initialValue": "", "in": {"$concat": ["$$value", "$$this"]}}}
                    .into(),
                doc! {"ws": ["a", "b", "c"]}
            ),
            Bson::String("abc".into())
        );
        // An empty input is the initial value; a null input is null.
        assert_eq!(
            on(
                doc! {"$reduce": {"input": "$e", "initialValue": 7, "in": 0}}.into(),
                doc! {"e": []}
            ),
            Bson::Int32(7)
        );
        assert_eq!(
            on(doc! {"$reduce": {"input": "$gone", "initialValue": 7, "in": 0}}.into(), doc! {}),
            Bson::Null
        );
        assert!(
            ev(doc! {"$reduce": {"input": "x", "initialValue": 0, "in": 0}}.into(), &d).is_err()
        );
    }

    #[test]
    fn reduce_initial_value_cannot_see_this() {
        // There is no element yet; the parser refuses rather than binding null.
        let expr = doc! {"$reduce": {"input": "$xs", "initialValue": "$$this", "in": 0}};
        assert!(Expr::parse(&expr.into()).is_err());
    }

    #[test]
    fn array_operators_compose_with_the_rest() {
        // Sum of the qty of every item over a threshold — the shape a report
        // actually needs, and the one that was impossible without a scope.
        let expr = doc! {"$reduce": {
            "input": {"$filter": {"input": "$items", "cond": {"$gt": ["$$this.qty", 1]}}},
            "initialValue": 0,
            "in": {"$add": ["$$value", "$$this.qty"]},
        }};
        let d = doc! {"items": [{"qty": 1}, {"qty": 2}, {"qty": 5}]};
        assert_eq!(on(expr.into(), d), Bson::Int64(7));
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

    // -- type conversion --------------------------------------------------

    fn err(expr: Document, d: Document) -> String {
        ev(expr.into(), &d).expect_err("expression should fail").to_string()
    }

    #[test]
    fn a_shorthand_takes_one_value_or_a_one_element_array() {
        assert_eq!(on(doc! {"$toInt": "$s"}.into(), doc! {"s": "42"}), Bson::Int32(42));
        assert_eq!(on(doc! {"$toInt": ["$s"]}.into(), doc! {"s": "42"}), Bson::Int32(42));
        assert!(Expr::parse(&doc! {"$toInt": ["$s", "$t"]}.into()).is_err());
    }

    #[test]
    fn convert_takes_a_type_name_or_its_code() {
        let by_name = doc! {"$convert": {"input": "7", "to": "int"}};
        let by_code = doc! {"$convert": {"input": "7", "to": 16}};
        assert_eq!(ok(by_name.into()), Bson::Int32(7));
        assert_eq!(ok(by_code.into()), Bson::Int32(7));
        assert_eq!(ok(doc! {"$convert": {"input": 7, "to": 2}}.into()), Bson::String("7".into()));
    }

    #[test]
    fn convert_refuses_a_malformed_specification_at_parse() {
        for spec in [
            doc! {"$convert": "int"},
            doc! {"$convert": {"to": "int"}},
            doc! {"$convert": {"input": 1}},
            doc! {"$convert": {"input": 1, "to": "int", "onerror": 0}},
            doc! {"$convert": {"input": 1, "to": "widget"}},
            // `number` is a `$type` alias for four types, not one conversion target.
            doc! {"$convert": {"input": 1, "to": "number"}},
            doc! {"$convert": {"input": 1, "to": 3}},
            doc! {"$convert": {"input": 1, "to": "$field"}},
        ] {
            assert!(Expr::parse(&spec.clone().into()).is_err(), "{spec:?} must not parse");
        }
    }

    #[test]
    fn convert_to_decimal_is_refused_with_a_reason() {
        // Decimal128 has no exact key encoding here (ADR-005), so a value
        // converted to it could be neither indexed nor grouped.
        let by_name = Expr::parse(&doc! {"$convert": {"input": 1, "to": "decimal"}}.into());
        let by_code = Expr::parse(&doc! {"$convert": {"input": 1, "to": 19}}.into());
        for result in [by_name, by_code] {
            let msg = result.expect_err("decimal must be refused").to_string();
            assert!(msg.contains("Decimal128"), "the refusal should say why: {msg}");
        }
    }

    #[test]
    fn a_null_or_missing_input_is_null_unless_on_null_says_otherwise() {
        assert_eq!(on(doc! {"$toInt": "$gone"}.into(), doc! {}), Bson::Null);
        assert_eq!(on(doc! {"$toInt": "$n"}.into(), doc! {"n": Bson::Null}), Bson::Null);
        let with_fallback = doc! {"$convert": {"input": "$gone", "to": "int", "onNull": 0}};
        assert_eq!(on(with_fallback.into(), doc! {}), Bson::Int32(0));
    }

    #[test]
    fn on_error_answers_an_unconvertible_value() {
        let expr = doc! {"$convert": {"input": "$s", "to": "int", "onError": -1}};
        assert_eq!(on(expr.clone().into(), doc! {"s": "abc"}), Bson::Int32(-1));
        assert_eq!(on(expr.into(), doc! {"s": "12"}), Bson::Int32(12));
        // Without it the error names both sides.
        let msg = err(doc! {"$toInt": "$s"}, doc! {"s": {"a": 1}});
        assert!(msg.contains("document") && msg.contains("int"), "{msg}");
    }

    #[test]
    fn the_fallbacks_are_lazy() {
        // A fallback that would fail is not evaluated when it is not needed.
        let on_null = doc! {"$convert": {"input": 5, "to": "long", "onNull": {"$divide": [1, 0]}}};
        assert_eq!(ok(on_null.into()), Bson::Int64(5));
        let on_error =
            doc! {"$convert": {"input": 5, "to": "long", "onError": {"$divide": [1, 0]}}};
        assert_eq!(ok(on_error.into()), Bson::Int64(5));
    }

    #[test]
    fn on_error_does_not_catch_an_error_in_the_input_expression() {
        // The fallback is for a value with no conversion, not for a broken
        // expression: hiding the latter would make a typo look like data.
        let expr = doc! {"$convert": {"input": {"$divide": [1, 0]}, "to": "int", "onError": 0}};
        assert!(ev(expr.into(), &Document::new()).is_err());
    }

    #[test]
    fn to_double_widens_numbers_and_parses_strings() {
        assert_eq!(ok(doc! {"$toDouble": 5}.into()), Bson::Double(5.0));
        assert_eq!(ok(doc! {"$toDouble": 5i64}.into()), Bson::Double(5.0));
        assert_eq!(ok(doc! {"$toDouble": true}.into()), Bson::Double(1.0));
        assert_eq!(ok(doc! {"$toDouble": "1.5"}.into()), Bson::Double(1.5));
        assert_eq!(ok(doc! {"$toDouble": "-1e3"}.into()), Bson::Double(-1000.0));
        assert_eq!(ok(doc! {"$toDouble": dt(1_500)}.into()), Bson::Double(1500.0));
        assert!(ev(doc! {"$toDouble": "1.5kg"}.into(), &Document::new()).is_err());
        // `$literal`, because a bare one-element array is the argument list.
        assert!(ev(doc! {"$toDouble": {"$literal": [1]}}.into(), &Document::new()).is_err());
    }

    #[test]
    fn to_int_truncates_toward_zero_and_refuses_what_does_not_fit() {
        assert_eq!(ok(doc! {"$toInt": 3.9}.into()), Bson::Int32(3));
        assert_eq!(ok(doc! {"$toInt": -3.9}.into()), Bson::Int32(-3));
        assert_eq!(ok(doc! {"$toInt": true}.into()), Bson::Int32(1));
        assert_eq!(ok(doc! {"$toInt": 7i64}.into()), Bson::Int32(7));
        assert_eq!(ok(doc! {"$toInt": "-42"}.into()), Bson::Int32(-42));
        // Out of range is an error, not a wrap: 2^40 has no int.
        assert!(err(doc! {"$toInt": 1_099_511_627_776i64}, doc! {}).contains("out of range"));
        assert!(err(doc! {"$toInt": 1.0e12}, doc! {}).contains("out of range"));
        assert!(ev(doc! {"$toInt": f64::NAN}.into(), &Document::new()).is_err());
        // A string is a base-10 integer or nothing: no prefixes, no decimals.
        assert!(ev(doc! {"$toInt": "1.5"}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$toInt": "12abc"}.into(), &Document::new()).is_err());
        // Epoch milliseconds never fit an int, so a date has no int form.
        assert!(ev(doc! {"$toInt": dt(0)}.into(), &Document::new()).is_err());
    }

    #[test]
    fn to_long_takes_dates_and_large_doubles() {
        assert_eq!(
            ok(doc! {"$toLong": dt(1_786_542_307_250)}.into()),
            Bson::Int64(1_786_542_307_250)
        );
        assert_eq!(ok(doc! {"$toLong": 1.0e18}.into()), Bson::Int64(1_000_000_000_000_000_000));
        assert_eq!(
            ok(doc! {"$toLong": "9007199254740993"}.into()),
            Bson::Int64(9_007_199_254_740_993)
        );
        assert_eq!(ok(doc! {"$toLong": 5}.into()), Bson::Int64(5));
        // 1e19 is past i64::MAX.
        assert!(ev(doc! {"$toLong": 1.0e19}.into(), &Document::new()).is_err());
    }

    #[test]
    fn to_string_renders_each_type_the_way_the_edge_does() {
        assert_eq!(ok(doc! {"$toString": 42}.into()), Bson::String("42".into()));
        assert_eq!(ok(doc! {"$toString": -7i64}.into()), Bson::String("-7".into()));
        assert_eq!(ok(doc! {"$toString": 1.5}.into()), Bson::String("1.5".into()));
        // A whole double prints without a fractional part, as MongoDB's does.
        assert_eq!(ok(doc! {"$toString": 2.0}.into()), Bson::String("2".into()));
        assert_eq!(ok(doc! {"$toString": true}.into()), Bson::String("true".into()));
        assert_eq!(
            ok(doc! {"$toString": dt(1_786_542_307_250)}.into()),
            Bson::String("2026-08-12T13:45:07.250Z".into())
        );
        let oid = bson::oid::ObjectId::new();
        assert_eq!(ok(doc! {"$toString": oid}.into()), Bson::String(oid.to_hex()));
        assert_eq!(ok(doc! {"$toString": f64::INFINITY}.into()), Bson::String("Infinity".into()));
        assert!(ev(doc! {"$toString": {"$literal": [1, 2]}}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$toString": {"a": 1}}.into(), &Document::new()).is_err());
    }

    #[test]
    fn to_bool_is_false_only_for_zero() {
        assert_eq!(ok(doc! {"$toBool": 0}.into()), Bson::Boolean(false));
        assert_eq!(ok(doc! {"$toBool": 0.0}.into()), Bson::Boolean(false));
        assert_eq!(ok(doc! {"$toBool": 0i64}.into()), Bson::Boolean(false));
        assert_eq!(ok(doc! {"$toBool": 5}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$toBool": false}.into()), Bson::Boolean(false));
        // MongoDB's rule, and a trap worth pinning: any string is true.
        assert_eq!(ok(doc! {"$toBool": "false"}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$toBool": ""}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$toBool": dt(0)}.into()), Bson::Boolean(true));
        assert_eq!(ok(doc! {"$toBool": {"$literal": [0]}}.into()), Bson::Boolean(true));
    }

    #[test]
    fn to_date_reads_epoch_milliseconds_and_iso_8601() {
        assert_eq!(ok(doc! {"$toDate": 1_500i64}.into()), dt(1_500));
        assert_eq!(ok(doc! {"$toDate": 1_500}.into()), dt(1_500));
        assert_eq!(ok(doc! {"$toDate": 1_500.9}.into()), dt(1_500));
        assert_eq!(ok(doc! {"$toDate": "2026-08-12T13:45:07.250Z"}.into()), dt(1_786_542_307_250));
        // An offset is honoured: 13:45:07 at +02:00 is 11:45:07Z.
        assert_eq!(
            ok(doc! {"$toDate": "2026-08-12T13:45:07.250+02:00"}.into()),
            dt(1_786_542_307_250 - 2 * 3_600_000)
        );
        // The looser spellings MongoDB takes: a bare date is midnight UTC, a
        // space may separate date and time, and no zone means UTC.
        assert_eq!(ok(doc! {"$toDate": "2026-08-12"}.into()), dt(1_786_492_800_000));
        assert_eq!(ok(doc! {"$toDate": "2026-08-12 13:45:07"}.into()), dt(1_786_542_307_000));
        assert_eq!(ok(doc! {"$toDate": "2026-08-12T13:45:07"}.into()), dt(1_786_542_307_000));
        assert!(ev(doc! {"$toDate": "yesterday"}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$toDate": true}.into(), &Document::new()).is_err());
    }

    #[test]
    fn to_date_of_an_object_id_is_its_creation_time() {
        // The leading four bytes are seconds since the epoch, big-endian.
        let oid = bson::oid::ObjectId::from_bytes([0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(ok(doc! {"$toDate": oid}.into()), dt(2_000));
    }

    #[test]
    fn to_object_id_parses_hex_and_passes_one_through() {
        let oid = bson::oid::ObjectId::new();
        assert_eq!(ok(doc! {"$toObjectId": oid.to_hex()}.into()), Bson::ObjectId(oid));
        assert_eq!(ok(doc! {"$toObjectId": oid}.into()), Bson::ObjectId(oid));
        assert!(ev(doc! {"$toObjectId": "not-hex"}.into(), &Document::new()).is_err());
        assert!(ev(doc! {"$toObjectId": 12}.into(), &Document::new()).is_err());
    }

    #[test]
    fn a_date_survives_a_round_trip_through_a_string() {
        let expr = doc! {"$toDate": {"$toString": "$t"}};
        assert_eq!(on(expr.into(), doc! {"t": dt(1_786_542_307_250)}), dt(1_786_542_307_250));
    }

    #[test]
    fn a_conversion_composes_with_arithmetic() {
        // The motivating shape: a quantity stored as text on some rows.
        let expr = doc! {"$multiply": [{"$toInt": "$qty"}, 2]};
        assert_eq!(on(expr.clone().into(), doc! {"qty": "21"}), Bson::Int64(42));
        assert_eq!(on(expr.into(), doc! {"qty": 21}), Bson::Int64(42));
    }
}

/// An expression's `$and` and `$or` fail only when the answer depends on an
/// argument that cannot be evaluated (ADR-211).
#[cfg(test)]
mod and_or {
    use super::*;
    use bson::{bson, doc};
    use proptest::prelude::*;

    /// The document every row is evaluated against: `qty` is the wrong type,
    /// `n` is zero.
    fn row() -> Document {
        doc! {"_id": 3, "qty": "twelve", "kind": "a", "n": 0, "tags": "red"}
    }

    fn ev(expr: Bson) -> Result<Bson> {
        Expr::parse(&expr)?.eval(&row())
    }

    /// Four arguments that cannot be evaluated, each failing differently.
    fn bads() -> Vec<Bson> {
        vec![
            bson!({"$add": ["$qty", 1]}),
            bson!({"$divide": [1, "$n"]}),
            bson!({"$range": [0, 200_000]}),
            bson!({"$switch": {"branches": [{"case": false, "then": 1}]}}),
        ]
    }

    /// What `bad` fails with on its own.
    fn message(bad: &Bson) -> String {
        ev(bad.clone()).expect_err("the argument should fail").to_string()
    }

    fn falsy() -> Vec<Bson> {
        vec![bson!(false), bson!(0), bson!(0.0), Bson::Null, bson!("$missing")]
    }

    fn truthy_values() -> Vec<Bson> {
        vec![bson!(true), bson!(1), bson!(""), bson!([]), bson!("text"), bson!({"a": 1})]
    }

    /// Both orders of a two-argument call.
    fn both_orders(op: &str, a: &Bson, b: &Bson) -> [Result<Bson>; 2] {
        [ev(bson!({op: [a.clone(), b.clone()]})), ev(bson!({op: [b.clone(), a.clone()]}))]
    }

    #[test]
    fn a_false_argument_decides_an_and_beside_one_that_cannot_be_evaluated() {
        for bad in bads() {
            for decider in falsy() {
                for got in both_orders("$and", &bad, &decider) {
                    assert_eq!(got.unwrap(), Bson::Boolean(false), "{bad} beside {decider}");
                }
            }
        }
    }

    #[test]
    fn a_true_argument_decides_an_or_beside_one_that_cannot_be_evaluated() {
        for bad in bads() {
            for decider in truthy_values() {
                for got in both_orders("$or", &bad, &decider) {
                    assert_eq!(got.unwrap(), Bson::Boolean(true), "{bad} beside {decider}");
                }
            }
        }
    }

    #[test]
    fn an_and_with_no_false_argument_fails_with_the_error() {
        for bad in bads() {
            for other in truthy_values() {
                for got in both_orders("$and", &bad, &other) {
                    assert_eq!(got.unwrap_err().to_string(), message(&bad), "{bad} beside {other}");
                }
            }
        }
    }

    #[test]
    fn an_or_with_no_true_argument_fails_with_the_error() {
        for bad in bads() {
            for other in falsy() {
                for got in both_orders("$or", &bad, &other) {
                    assert_eq!(got.unwrap_err().to_string(), message(&bad), "{bad} beside {other}");
                }
            }
        }
    }

    #[test]
    fn with_no_error_the_answers_are_unchanged() {
        assert_eq!(ev(bson!({"$and": [true, 1, "x"]})).unwrap(), Bson::Boolean(true));
        assert_eq!(ev(bson!({"$and": [true, 0]})).unwrap(), Bson::Boolean(false));
        assert_eq!(ev(bson!({"$or": [false, null, 0]})).unwrap(), Bson::Boolean(false));
        assert_eq!(ev(bson!({"$or": [false, []]})).unwrap(), Bson::Boolean(true));
        assert_eq!(ev(bson!({"$and": "$kind"})).unwrap(), Bson::Boolean(true));
        assert!(Expr::parse(&bson!({"$and": []})).is_err(), "an empty $and is refused at parse");
        assert!(Expr::parse(&bson!({"$or": []})).is_err(), "an empty $or is refused at parse");
    }

    #[test]
    fn the_answer_is_a_boolean_not_the_argument_that_decided() {
        assert_eq!(ev(bson!({"$and": [0, {"$add": ["$qty", 1]}]})).unwrap(), Bson::Boolean(false));
        assert_eq!(
            ev(bson!({"$and": [{"$add": ["$qty", 1]}, null]})).unwrap(),
            Bson::Boolean(false)
        );
        assert_eq!(ev(bson!({"$or": ["x", {"$add": ["$qty", 1]}]})).unwrap(), Bson::Boolean(true));
        assert_eq!(ev(bson!({"$or": [{"$add": ["$qty", 1]}, 7]})).unwrap(), Bson::Boolean(true));
    }

    #[test]
    fn the_error_reported_is_the_earliest_written() {
        let [b1, b2, ..] = &bads()[..] else { unreachable!() };
        assert_ne!(message(b1), message(b2));
        let and = |args: Vec<Bson>| ev(bson!({"$and": args})).unwrap_err().to_string();
        let or = |args: Vec<Bson>| ev(bson!({"$or": args})).unwrap_err().to_string();
        assert_eq!(and(vec![b1.clone(), b2.clone(), bson!(true)]), message(b1));
        assert_eq!(and(vec![b1.clone(), bson!(true), b2.clone()]), message(b1));
        assert_eq!(and(vec![bson!(true), b2.clone(), b1.clone()]), message(b2));
        assert_eq!(or(vec![b2.clone(), bson!(false), b1.clone()]), message(b2));
        assert_eq!(or(vec![bson!(false), b1.clone(), b2.clone()]), message(b1));
    }

    #[test]
    fn nesting_composes_as_three_valued_logic() {
        let bad = bson!({"$add": ["$qty", 1]});
        let and_bad_true = bson!({"$and": [bad.clone(), true]});
        let and_bad_false = bson!({"$and": [bad.clone(), false]});
        assert_eq!(
            ev(bson!({"$or": [and_bad_false.clone(), false]})).unwrap(),
            Bson::Boolean(false)
        );
        assert_eq!(ev(bson!({"$or": [and_bad_true.clone(), true]})).unwrap(), Bson::Boolean(true));
        assert!(ev(bson!({"$or": [and_bad_true.clone(), false]})).is_err());
        assert!(ev(bson!({"$not": [and_bad_true]})).is_err(), "$not of not known is not known");
        assert_eq!(ev(bson!({"$not": [and_bad_false]})).unwrap(), Bson::Boolean(true));
        assert_eq!(
            ev(bson!({"$cond": [{"$or": [bad.clone(), true]}, 1, 2]})).unwrap(),
            Bson::Int32(1)
        );
        // `$cond` itself is unchanged: a condition that cannot be evaluated
        // fails it even when both branches agree.
        assert!(ev(bson!({"$cond": [bad, 1, 1]})).is_err());
    }

    /// The guard `aggregation.md` recommends, written with `$and`.
    #[test]
    fn a_type_guard_written_with_and_protects_what_follows_it() {
        let guard = bson!({"$and": [{"$isArray": "$tags"}, {"$gt": [{"$size": "$tags"}, 2]}]});
        assert_eq!(ev(guard.clone()).unwrap(), Bson::Boolean(false));
        let d = doc! {"tags": ["a", "b", "c"]};
        assert_eq!(Expr::parse(&guard).unwrap().eval(&d).unwrap(), Bson::Boolean(true));
    }

    /// How many `$and`/`$or` arguments `expr` evaluates.
    fn arguments_evaluated(expr: Bson) -> (Result<Bson>, usize) {
        let parsed = Expr::parse(&expr).unwrap();
        let before = logic_arguments();
        let got = parsed.eval(&row());
        (got, logic_arguments() - before)
    }

    #[test]
    fn evaluation_stops_at_the_argument_that_decides() {
        let bad = bson!({"$add": ["$qty", 1]});
        assert_eq!(arguments_evaluated(bson!({"$and": [true, false, bad.clone(), true]})).1, 2);
        assert_eq!(arguments_evaluated(bson!({"$and": [false, bad.clone()]})).1, 1);
        assert_eq!(arguments_evaluated(bson!({"$or": [false, true, bad.clone(), false]})).1, 2);
        assert_eq!(arguments_evaluated(bson!({"$or": [1, bad.clone()]})).1, 1);
        // Nothing decides: every argument is read.
        assert_eq!(arguments_evaluated(bson!({"$and": [true, true, true]})).1, 3);
    }

    #[test]
    fn evaluation_goes_on_past_an_error_until_an_argument_decides() {
        let bad = bson!({"$add": ["$qty", 1]});
        let (got, n) = arguments_evaluated(bson!({"$and": [bad.clone(), true, false, true]}));
        assert_eq!((got.unwrap(), n), (Bson::Boolean(false), 3));
        let (got, n) = arguments_evaluated(bson!({"$or": [bad.clone(), false, true, false]}));
        assert_eq!((got.unwrap(), n), (Bson::Boolean(true), 3));
        let (got, n) = arguments_evaluated(bson!({"$and": [bad, true, true]}));
        assert!(got.is_err());
        assert_eq!(n, 3);
    }

    #[test]
    fn a_filter_limit_still_stops_early_over_a_deciding_and() {
        let expr = bson!({"$filter": {
            "input": [1, 2, 3, 4],
            "as": "i",
            "cond": {"$and": [{"$gt": ["$$i", 0]}, true]},
            "limit": 2,
        }});
        let (got, n) = arguments_evaluated(expr);
        assert_eq!(got.unwrap(), bson!([1, 2]));
        assert_eq!(n, 4, "two elements, two arguments each");
    }

    /// A variable nothing bound is a bug in the caller, not a value with no
    /// answer: it fails the `$or` even when another argument is true.
    #[test]
    fn an_unbound_variable_is_never_set_aside() {
        let vars = ["x".to_string()];
        for (op, decider) in [("$or", bson!(true)), ("$and", bson!(false))] {
            for args in [vec![bson!("$$x"), decider.clone()], vec![decider.clone(), bson!("$$x")]] {
                let written = args.clone();
                let parsed = Expr::parse_with_vars(&bson!({op: args}), &vars).unwrap();
                let got = parsed.eval(&row());
                if written[0] == decider {
                    // The decider is reached first, so the variable is never read.
                    assert_eq!(got.unwrap(), Bson::Boolean(truthy(&decider)), "{op} {written:?}");
                } else {
                    let e = got.expect_err("an unbound variable must fail");
                    assert!(matches!(e, Error::Internal(_)), "{op} {written:?}: {e}");
                    assert!(!e.is_deferrable());
                }
            }
        }
    }

    #[test]
    fn the_shared_scan_returns_an_error_that_cannot_wait_at_once() {
        let mut seen = 0;
        let items: Vec<Result<bool>> = vec![
            Err(Error::InvalidQuery("value".into())),
            Err(Error::Internal("invariant".into())),
            Ok(false),
        ];
        let got = all_of(items, |r| {
            seen += 1;
            r
        });
        assert!(matches!(got, Err(Error::Internal(_))), "{got:?}");
        assert_eq!(seen, 2, "nothing after it is evaluated");
    }

    /// One argument of a generated `$and`/`$or`.
    #[derive(Clone, Debug)]
    enum Arg {
        True(usize),
        False(usize),
        /// Fails with a message of its own.
        Error(usize),
    }

    impl Arg {
        fn expr(&self) -> Bson {
            match *self {
                Arg::True(i) => truthy_values()[i % truthy_values().len()].clone(),
                Arg::False(i) => falsy()[i % falsy().len()].clone(),
                // Each `k` names a different count in the refusal.
                Arg::Error(k) => bson!({"$range": [0, 100_001 + k as i64]}),
            }
        }
    }

    fn arg() -> impl Strategy<Value = Arg> {
        prop_oneof![
            (0..6usize).prop_map(Arg::True),
            (0..5usize).prop_map(Arg::False),
            (0..4usize).prop_map(Arg::Error),
        ]
    }

    /// The reference: three-valued logic, order-free, with the
    /// earliest-written error, and how many arguments a scan in written order
    /// evaluates before one decides.
    fn model(is_and: bool, args: &[Arg]) -> (std::result::Result<bool, usize>, usize) {
        let decides =
            |a: &Arg| matches!((is_and, a), (true, Arg::False(_)) | (false, Arg::True(_)));
        let evaluated = args.iter().position(decides).map_or(args.len(), |i| i + 1);
        if args.iter().any(decides) {
            return (Ok(!is_and), evaluated);
        }
        match args.iter().find_map(|a| if let Arg::Error(k) = a { Some(*k) } else { None }) {
            Some(k) => (Err(k), evaluated),
            None => (Ok(is_and), evaluated),
        }
    }

    proptest! {
        #[test]
        fn and_and_or_match_three_valued_logic(
            is_and in any::<bool>(),
            args in prop::collection::vec(arg(), 1..7),
        ) {
            let op = if is_and { "$and" } else { "$or" };
            let exprs: Vec<Bson> = args.iter().map(Arg::expr).collect();
            let (got, n) = arguments_evaluated(bson!({op: exprs}));
            let (want, want_n) = model(is_and, &args);
            match want {
                Ok(b) => prop_assert_eq!(got.unwrap(), Bson::Boolean(b)),
                Err(k) => prop_assert_eq!(
                    got.unwrap_err().to_string(),
                    message(&Arg::Error(k).expr())
                ),
            }
            prop_assert_eq!(n, want_n);
        }
    }
}

/// Random nested `$and`/`$or`/`$not`/`$cond` trees over literal values,
/// distinct value errors and an unbound variable, against a naive
/// three-valued reference that also counts the `$and`/`$or` arguments
/// evaluated (ADR-211).
#[cfg(test)]
mod and_or_nested {
    use super::*;
    use bson::bson;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Node {
        /// Index into `values()`.
        Lit(usize),
        /// A deferrable value error with a message of its own.
        Bad(usize),
        /// `$$x`, parsed as bound, evaluated unbound: `Error::Internal`.
        Unbound,
        And(Vec<Node>),
        Or(Vec<Node>),
        Not(Box<Node>),
        Cond(Box<Node>, Box<Node>, Box<Node>),
    }

    fn values() -> Vec<Bson> {
        vec![
            bson!(true),
            bson!(false),
            bson!(1),
            bson!(0),
            Bson::Null,
            bson!(""),
            bson!([]),
            bson!(0.0),
            bson!("x"),
            bson!("$missing"),
        ]
    }

    fn to_bson(n: &Node) -> Bson {
        match n {
            Node::Lit(i) => values()[*i].clone(),
            Node::Bad(k) => bson!({"$range": [0, 100_001 + *k as i64]}),
            Node::Unbound => bson!("$$x"),
            Node::And(a) => bson!({"$and": a.iter().map(to_bson).collect::<Vec<_>>()}),
            Node::Or(a) => bson!({"$or": a.iter().map(to_bson).collect::<Vec<_>>()}),
            Node::Not(x) => bson!({"$not": [to_bson(x)]}),
            Node::Cond(c, t, e) => bson!({"$cond": [to_bson(c), to_bson(t), to_bson(e)]}),
        }
    }

    /// What the reference returns: a value, a deferrable error `k`, or the
    /// internal error.
    #[derive(Clone, Debug, PartialEq)]
    enum Out {
        Val(Bson),
        Bad(usize),
        Internal,
    }

    fn lit_value(i: usize) -> Bson {
        // A missing field reads as null.
        match values()[i].clone() {
            Bson::String(s) if s == "$missing" => Bson::Null,
            v => v,
        }
    }

    /// Naive three-valued reference. Returns (result, $and/$or arguments evaluated).
    fn reference(n: &Node) -> (Out, usize) {
        match n {
            Node::Lit(i) => (Out::Val(lit_value(*i)), 0),
            Node::Bad(k) => (Out::Bad(*k), 0),
            Node::Unbound => (Out::Internal, 0),
            Node::Not(x) => match reference(x) {
                (Out::Val(v), c) => (Out::Val(Bson::Boolean(!truthy(&v))), c),
                other => other,
            },
            Node::Cond(c, t, e) => {
                let (cv, cc) = reference(c);
                match cv {
                    Out::Val(v) => {
                        let (bv, bc) = if truthy(&v) { reference(t) } else { reference(e) };
                        (bv, cc + bc)
                    }
                    other => (other, cc),
                }
            }
            Node::And(args) | Node::Or(args) => {
                let decider = matches!(n, Node::Or(_));
                let mut count = 0;
                let mut first_bad: Option<usize> = None;
                for a in args {
                    let (r, c) = reference(a);
                    count += 1 + c;
                    match r {
                        Out::Val(v) if truthy(&v) == decider => {
                            return (Out::Val(Bson::Boolean(decider)), count);
                        }
                        Out::Val(_) => {}
                        Out::Internal => return (Out::Internal, count),
                        Out::Bad(k) => {
                            first_bad.get_or_insert(k);
                        }
                    }
                }
                match first_bad {
                    Some(k) => (Out::Bad(k), count),
                    None => (Out::Val(Bson::Boolean(!decider)), count),
                }
            }
        }
    }

    fn node() -> impl Strategy<Value = Node> {
        let leaf = prop_oneof![
            6 => (0..10usize).prop_map(Node::Lit),
            3 => (0..5usize).prop_map(Node::Bad),
            1 => Just(Node::Unbound),
        ];
        leaf.prop_recursive(5, 48, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 1..5).prop_map(Node::And),
                prop::collection::vec(inner.clone(), 1..5).prop_map(Node::Or),
                inner.clone().prop_map(|x| Node::Not(Box::new(x))),
                (inner.clone(), inner.clone(), inner).prop_map(|(c, t, e)| Node::Cond(
                    Box::new(c),
                    Box::new(t),
                    Box::new(e)
                )),
            ]
        })
    }

    fn row() -> Document {
        bson::doc! {"_id": 1}
    }

    fn actual(n: &Node) -> (Result<Bson>, usize) {
        let parsed = Expr::parse_with_vars(&to_bson(n), &["x".to_string()]).unwrap();
        let before = logic_arguments();
        let got = parsed.eval(&row());
        (got, logic_arguments() - before)
    }

    fn bad_message(k: usize) -> String {
        Expr::parse(&to_bson(&Node::Bad(k))).unwrap().eval(&row()).unwrap_err().to_string()
    }

    /// `tree` with every unbound variable replaced by `true`.
    fn without_unbound(n: Node) -> Node {
        match n {
            Node::Unbound => Node::Lit(0),
            Node::Lit(_) | Node::Bad(_) => n,
            Node::And(a) => Node::And(a.into_iter().map(without_unbound).collect()),
            Node::Or(a) => Node::Or(a.into_iter().map(without_unbound).collect()),
            Node::Not(x) => Node::Not(Box::new(without_unbound(*x))),
            Node::Cond(c, t, e) => Node::Cond(
                Box::new(without_unbound(*c)),
                Box::new(without_unbound(*t)),
                Box::new(without_unbound(*e)),
            ),
        }
    }

    /// Enough to reach depth five often, and quick enough to run with the
    /// rest of the suite.
    const CASES: u32 = 4000;

    proptest! {
        #![proptest_config(ProptestConfig { cases: CASES, .. ProptestConfig::default() })]

        #[test]
        fn nested_trees_match_the_naive_three_valued_reference(tree in node()) {
            let (want, want_n) = reference(&tree);
            let (got, n) = actual(&tree);
            match (&want, &got) {
                (Out::Val(v), Ok(g)) => prop_assert_eq!(g, v, "{}", to_bson(&tree)),
                (Out::Bad(k), Err(e)) => {
                    prop_assert!(e.is_deferrable(), "{}", to_bson(&tree));
                    prop_assert_eq!(e.to_string(), bad_message(*k), "{}", to_bson(&tree));
                }
                (Out::Internal, Err(Error::Internal(m))) => {
                    prop_assert_eq!(m.as_str(), "variable $$x is not bound")
                }
                _ => prop_assert!(false, "{}: want {:?}, got {:?}", to_bson(&tree), want, got),
            }
            prop_assert_eq!(n, want_n, "argument count for {}", to_bson(&tree));
        }

        /// Any reordering of the arguments of every `$and`/`$or` leaves a
        /// value answer unchanged (no Internal leaf, which is order-dependent
        /// by design).
        #[test]
        fn a_value_answer_does_not_depend_on_argument_order(
            tree in node().prop_map(without_unbound),
            seed in any::<u64>(),
        ) {
            fn rand(seed: &mut u64) -> usize {
                *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (*seed >> 33) as usize
            }
            fn shuffle(n: &Node, seed: &mut u64) -> Node {
                match n {
                    Node::And(a) | Node::Or(a) => {
                        let mut v: Vec<Node> = a.iter().map(|x| shuffle(x, seed)).collect();
                        for i in (1..v.len()).rev() {
                            let j = rand(seed) % (i + 1);
                            v.swap(i, j);
                        }
                        if matches!(n, Node::And(_)) { Node::And(v) } else { Node::Or(v) }
                    }
                    Node::Not(x) => Node::Not(Box::new(shuffle(x, seed))),
                    Node::Cond(c, t, e) => Node::Cond(
                        Box::new(shuffle(c, seed)),
                        Box::new(shuffle(t, seed)),
                        Box::new(shuffle(e, seed)),
                    ),
                    other => other.clone(),
                }
            }
            let mut s = seed;
            let shuffled = shuffle(&tree, &mut s);
            let (a, _) = actual(&tree);
            let (b, _) = actual(&shuffled);
            prop_assert_eq!(a.is_ok(), b.is_ok(), "{} vs {}", to_bson(&tree), to_bson(&shuffled));
            if let (Ok(x), Ok(y)) = (a, b) {
                prop_assert_eq!(x, y);
            }
        }
    }

    /// Nesting across `$not` and `$cond`, written out.
    #[test]
    fn hand_rows() {
        let ev = |b: Bson| Expr::parse(&b).unwrap().eval(&bson::doc! {"qty": "t"});
        let bad = bson!({"$add": ["$qty", 1]});
        // `$or` inside `$not` inside `$and`.
        assert_eq!(
            ev(bson!({"$and": [{"$not": [{"$or": [bad.clone(), true]}]}, bad.clone()]})).unwrap(),
            Bson::Boolean(false)
        );
        assert!(ev(bson!({"$and": [{"$not": [{"$or": [bad.clone(), false]}]}, false]})).is_ok());
        assert!(ev(bson!({"$and": [{"$not": [{"$or": [bad.clone(), false]}]}, true]})).is_err());
        // An error deferred across a `$cond` boundary: the condition's `$and`
        // defers an error that a later false decides, and the branch taken
        // decides an outer `$or`.
        assert_eq!(
            ev(bson!({"$or": [bad.clone(), {"$cond": [{"$and": [bad.clone(), false]}, 0, 1]}]}))
                .unwrap(),
            Bson::Boolean(true)
        );
        // `$cond` itself is still strict on its condition.
        assert!(ev(bson!({"$or": [false, {"$cond": [bad.clone(), 1, 1]}]})).is_err());
        // But an `$or` beside it with a true argument hides it.
        assert_eq!(
            ev(bson!({"$or": [{"$cond": [bad, 1, 1]}, true]})).unwrap(),
            Bson::Boolean(true)
        );
    }
}

#[cfg(test)]
mod decimal128 {
    use super::*;
    use bson::doc;

    #[test]
    fn a_decimal128_literal_is_refused_wherever_an_expression_would_hold_one() {
        // The mirror of `$convert` refusing `decimal` as a target: a literal
        // Decimal128 would compare equal to every number and could be
        // neither indexed nor grouped, so it is refused at parse — bare, as
        // `$literal`, and inside a document or array literal.
        let d = Bson::Decimal128("1.5".parse().unwrap());
        for expr in [
            Bson::Document(doc! { "$eq": ["$v", d.clone()] }),
            Bson::Document(doc! { "$literal": d.clone() }),
            Bson::Document(doc! { "$literal": { "n": d.clone() } }),
            Bson::Document(doc! { "$in": [d.clone(), "$tags"] }),
            Bson::Document(doc! { "$add": ["$v", d.clone()] }),
            Bson::Document(doc! { "n": d.clone() }),
            Bson::Array(vec![Bson::Int32(1), d.clone()]),
            d.clone(),
        ] {
            let Err(err) = Expr::parse(&expr) else { panic!("{expr:?} should be refused") };
            let msg = err.to_string();
            assert!(
                msg.contains("Decimal128 literal"),
                "{expr:?}: the refusal should say why: {msg}"
            );
        }
        assert!(Expr::parse(&Bson::Document(doc! { "$eq": ["$v", 1.5] })).is_ok());
    }

    fn dec(text: &str) -> bson::Decimal128 {
        text.parse().unwrap()
    }

    /// A `Decimal128` from its 128 bits, in the specification's bit order
    /// (sign first); BSON stores them little-endian.
    fn raw(bits: u128) -> bson::Decimal128 {
        bson::Decimal128::from_bytes(bits.to_le_bytes())
    }

    /// Zeros of every sign and of exponents at both ends and between.
    const ZEROS: [&str; 8] = ["0", "-0", "0.000", "-0.0", "0E-6176", "-0E-6176", "0E+6111", "0E+3"];

    /// Not zero: the smallest and largest magnitudes, both signs, `NaN` and
    /// the infinities.
    const NON_ZEROS: [&str; 9] = [
        "1",
        "-1",
        "0.001",
        "1E-6176",
        "-1E-6176",
        "9.999999999999999999999999999999999E+6144",
        "NaN",
        "Infinity",
        "-Infinity",
    ];

    #[test]
    fn a_decimal128_zero_of_any_sign_or_exponent_is_false() {
        for text in ZEROS {
            assert!(!truthy(&Bson::Decimal128(dec(text))), "{text} should be false");
        }
        for text in NON_ZEROS {
            assert!(truthy(&Bson::Decimal128(dec(text))), "{text} should be true");
        }
        // A signalling NaN (`0 11111 1…`) and a negative NaN (`1 11111 0…`)
        // are NaNs, and true.
        assert!(truthy(&Bson::Decimal128(raw(0x7e_u128 << 120))));
        assert!(truthy(&Bson::Decimal128(raw(0xfc_u128 << 120))));
    }

    #[test]
    fn a_non_canonical_decimal128_coefficient_reads_as_zero() {
        // IEEE 754-2008 reads a coefficient above `10^34 - 1` as zero, and so
        // does the `bson` crate's own text. Two ways to write one: the `11`
        // form, whose coefficient starts at `2^113`, and the plain form with
        // all 113 coefficient bits set.
        let eleven_form = raw((0b0_11u128 << 125) | 1);
        let too_big = raw((1u128 << 113) - 1);
        for d in [eleven_form, too_big] {
            assert!(decimal128_is_zero(&d), "{d}");
            assert!(!truthy(&Bson::Decimal128(d)));
            assert!(d.to_string().starts_with('0'), "the crate reads {d} as zero too");
        }
        // One below the limit is the largest canonical coefficient.
        let largest = raw(9_999_999_999_999_999_999_999_999_999_999_999u128);
        assert!(!decimal128_is_zero(&largest));
    }

    /// The crate's own reading of a `Decimal128`'s text: finite, and every
    /// digit of its coefficient a zero.
    fn zero_by_its_text(d: &bson::Decimal128) -> bool {
        let text = d.to_string();
        if text.contains("NaN") || text.contains("Infinity") {
            return false;
        }
        let coefficient = text.split('E').next().unwrap();
        coefficient.chars().filter(char::is_ascii_digit).all(|c| c == '0')
    }

    proptest::proptest! {
        #[test]
        fn the_bit_reading_of_zero_agrees_with_the_crates_text(
            bits in proptest::prelude::any::<u128>(),
            // Most random bit patterns are far from zero; this half lands
            // on a zero coefficient, at a random sign and exponent.
            zero_coefficient in proptest::prelude::any::<bool>(),
        ) {
            let bits = if zero_coefficient { bits & !((1u128 << 113) - 1) } else { bits };
            let d = raw(bits);
            proptest::prop_assert_eq!(decimal128_is_zero(&d), zero_by_its_text(&d), "{}", d);
        }
    }

    fn on(expr: Document, d: Document) -> Result<Bson> {
        Expr::parse(&Bson::Document(expr))?.eval(&d)
    }

    #[test]
    fn a_stored_decimal128_zero_is_false_wherever_an_expression_reads_truth() {
        for (text, truth) in
            ZEROS.iter().map(|t| (*t, false)).chain(NON_ZEROS.iter().map(|t| (*t, true)))
        {
            let q = Bson::Decimal128(dec(text));
            let d = doc! { "q": q.clone(), "items": [q.clone(), 1], "pair": [q, false] };
            let cond = on(doc! { "$cond": ["$q", "yes", "no"] }, d.clone()).unwrap();
            assert_eq!(cond, Bson::from(if truth { "yes" } else { "no" }), "{text} in $cond");
            for (expr, want) in [
                (doc! { "$and": ["$q", true] }, truth),
                (doc! { "$or": ["$q", false] }, truth),
                (doc! { "$not": ["$q"] }, !truth),
                (doc! { "$toBool": "$q" }, truth),
                (doc! { "$convert": { "input": "$q", "to": "bool" } }, truth),
                (doc! { "$anyElementTrue": "$pair" }, truth),
                (doc! { "$allElementsTrue": "$items" }, truth),
                (
                    doc! { "$switch": { "branches": [{ "case": "$q", "then": true }], "default": false } },
                    truth,
                ),
            ] {
                assert_eq!(
                    on(expr.clone(), d.clone()).unwrap(),
                    Bson::Boolean(want),
                    "{text}: {expr}"
                );
            }
            let kept = on(doc! { "$filter": { "input": "$items", "cond": "$$this" } }, d).unwrap();
            let want = if truth { 2 } else { 1 };
            assert_eq!(kept.as_array().unwrap().len(), want, "{text} in $filter: {kept}");
        }
    }
}

#[cfg(test)]
mod sets_and_pairs {
    use super::*;
    use bson::doc;

    fn ev(expr: Document, d: &Document) -> Result<Bson> {
        Expr::parse(&Bson::Document(expr))?.eval(d)
    }

    fn on(expr: Document, d: Document) -> Bson {
        ev(expr, &d).unwrap_or_else(|e| panic!("{e}"))
    }

    fn ok(expr: Document) -> Bson {
        on(expr, doc! {})
    }

    fn refused(expr: Document, d: Document) -> String {
        match ev(expr.clone(), &d) {
            Ok(v) => panic!("{expr:?} should be refused, gave {v:?}"),
            Err(e) => e.to_string(),
        }
    }

    /// The exact elements, their BSON types included: `1` and `1.0` print
    /// differently in `Debug`, so a kept representative is checked too.
    fn exactly(got: &Bson, want: &[Bson]) {
        assert_eq!(format!("{got:?}"), format!("{:?}", Bson::Array(want.to_vec())));
    }

    #[test]
    fn the_worked_example_unions_tags_and_wants_first_seen() {
        let d = doc! { "tags": ["vip", "eu"], "wants": ["eu", "newsletter"] };
        let got = on(doc! { "$setUnion": ["$tags", "$wants"] }, d);
        exactly(&got, &["vip".into(), "eu".into(), "newsletter".into()]);
        let d = doc! { "tags": ["trial"], "wants": ["trial"] };
        exactly(&on(doc! { "$setUnion": ["$tags", "$wants"] }, d), &["trial".into()]);
    }

    #[test]
    fn union_keeps_one_of_each_member_first_seen_and_untouched() {
        // Duplicates inside one array and across arrays collapse; the member
        // kept is the first appearance, still an Int32.
        let got = on(
            doc! { "$setUnion": ["$a", "$b", "$c"] },
            doc! { "a": [3, 1, 3], "b": [1.0, 2_i64], "c": [2.0, 4] },
        );
        exactly(&got, &[Bson::Int32(3), Bson::Int32(1), Bson::Int64(2), Bson::Int32(4)]);
        exactly(&ok(doc! { "$setUnion": [[], []] }), &[]);
        // One argument is the distinct members of that array.
        exactly(&on(doc! { "$setUnion": "$a" }, doc! { "a": [2, 2, 1] }), &[2.into(), 1.into()]);
    }

    #[test]
    fn intersection_is_the_first_arrays_members_every_other_holds() {
        let got = on(
            doc! { "$setIntersection": ["$a", "$b"] },
            doc! { "a": [3, 1, 2, 1, 2.0], "b": [2.0, 1_i64, 9] },
        );
        exactly(&got, &[Bson::Int32(1), Bson::Int32(2)]);
        let got = on(
            doc! { "$setIntersection": ["$a", "$b", "$c"] },
            doc! { "a": [1, 2, 3], "b": [3, 2], "c": [3, 1] },
        );
        exactly(&got, &[Bson::Int32(3)]);
        exactly(&ok(doc! { "$setIntersection": [[1, 2], []] }), &[]);
        exactly(&ok(doc! { "$setIntersection": [[], [1]] }), &[]);
    }

    #[test]
    fn difference_is_the_first_arrays_members_the_second_lacks() {
        let got =
            on(doc! { "$setDifference": ["$a", "$b"] }, doc! { "a": [1, 2, 3, 1], "b": [2.0] });
        exactly(&got, &[Bson::Int32(1), Bson::Int32(3)]);
        exactly(&ok(doc! { "$setDifference": [[2, 2, 1], []] }), &[2.into(), 1.into()]);
        exactly(&ok(doc! { "$setDifference": [[], [1]] }), &[]);
    }

    #[test]
    fn equals_and_subset_ignore_duplicates_order_and_numeric_width() {
        let d = doc! { "a": [1, 2], "b": [2.0, 1_i64, 1], "c": [1, 2, 3], "e": [] };
        assert_eq!(on(doc! { "$setEquals": ["$a", "$b"] }, d.clone()), true.into());
        assert_eq!(on(doc! { "$setEquals": ["$a", "$b", "$a"] }, d.clone()), true.into());
        assert_eq!(on(doc! { "$setEquals": ["$a", "$c"] }, d.clone()), false.into());
        // As many members, but not the same ones.
        assert_eq!(ok(doc! { "$setEquals": [[1, 2], [1, 3, 3]] }), false.into());
        assert_eq!(on(doc! { "$setEquals": ["$c", "$a"] }, d.clone()), false.into());
        assert_eq!(on(doc! { "$setEquals": ["$a", "$b", "$c"] }, d.clone()), false.into());
        assert_eq!(on(doc! { "$setEquals": ["$e", "$e"] }, d.clone()), true.into());
        assert_eq!(on(doc! { "$setIsSubset": ["$b", "$a"] }, d.clone()), true.into());
        assert_eq!(on(doc! { "$setIsSubset": ["$a", "$c"] }, d.clone()), true.into());
        assert_eq!(on(doc! { "$setIsSubset": ["$c", "$a"] }, d.clone()), false.into());
        assert_eq!(on(doc! { "$setIsSubset": ["$e", "$a"] }, d.clone()), true.into());
        assert_eq!(on(doc! { "$setIsSubset": ["$a", "$e"] }, d), false.into());
    }

    #[test]
    fn the_set_operators_pass_null_and_missing_through_from_any_position() {
        for op in ["$setUnion", "$setIntersection", "$setDifference", "$setEquals", "$setIsSubset"]
        {
            for args in [
                vec![Bson::from("$missing"), Bson::from("$a")],
                vec![Bson::from("$a"), Bson::from("$missing")],
                vec![Bson::from("$a"), Bson::Null],
                vec![Bson::Null, Bson::Null],
            ] {
                let mut expr = Document::new();
                expr.insert(op, args.clone());
                assert_eq!(on(expr, doc! { "a": [1] }), Bson::Null, "{op} {args:?}");
            }
        }
    }

    #[test]
    fn a_non_array_is_refused_wherever_it_stands_even_beside_a_null() {
        for op in ["$setUnion", "$setIntersection", "$setDifference", "$setEquals", "$setIsSubset"]
        {
            for args in [
                vec![Bson::from("$s"), Bson::Null],
                vec![Bson::Null, Bson::from("$s")],
                vec![Bson::from("$a"), Bson::from("$s")],
            ] {
                let mut expr = Document::new();
                expr.insert(op, args.clone());
                let msg = refused(expr, doc! { "a": [1], "s": "text" });
                assert!(
                    msg.contains(op) && msg.contains("needs an array, found a string"),
                    "{op} {args:?}: {msg}"
                );
            }
        }
    }

    #[test]
    fn arity_is_checked_at_parse() {
        for (expr, want) in [
            (doc! { "$setDifference": [[1], [2], [3]] }, "exactly 2"),
            (doc! { "$setIsSubset": [[1]] }, "exactly 2"),
            (doc! { "$setEquals": [[1]] }, "at least 2"),
            (doc! { "$setUnion": [] }, "at least 1"),
            (doc! { "$anyElementTrue": [true, false] }, "exactly 1"),
            (doc! { "$arrayToObject": [["a", 1], ["b", 2]] }, "exactly 1"),
        ] {
            let msg = Expr::parse(&Bson::Document(expr.clone())).unwrap_err().to_string();
            assert!(msg.contains(want), "{expr:?}: {msg}");
        }
    }

    #[test]
    fn nested_arrays_and_documents_are_members_compared_whole() {
        let d = doc! {
            "a": [[1, 2], { "x": 1 }, { "x": 1, "y": 2 }],
            "b": [[1.0, 2], { "x": 1.0 }, { "y": 2, "x": 1 }, [2, 1]],
        };
        // `[1, 2]` and `[1.0, 2]` are one member, `{x: 1}` and `{x: 1.0}` are
        // one, and two documents with their keys in a different order are two.
        let got = on(doc! { "$setUnion": ["$a", "$b"] }, d.clone());
        exactly(
            &got,
            &[
                Bson::Array(vec![1.into(), 2.into()]),
                Bson::Document(doc! { "x": 1 }),
                Bson::Document(doc! { "x": 1, "y": 2 }),
                Bson::Document(doc! { "y": 2, "x": 1 }),
                Bson::Array(vec![2.into(), 1.into()]),
            ],
        );
        let got = on(doc! { "$setIntersection": ["$a", "$b"] }, d);
        exactly(&got, &[Bson::Array(vec![1.into(), 2.into()]), Bson::Document(doc! { "x": 1 })]);
        // An array element is a member, not flattened into its contents.
        exactly(&ok(doc! { "$setIntersection": [[[1]], [1]] }), &[]);
        assert_eq!(ok(doc! { "$setIsSubset": [[[1]], [1]] }), false.into());
    }

    /// Every way two values can be one member or two, without a Decimal128.
    fn corpus() -> Vec<Bson> {
        vec![
            Bson::Double(f64::NAN),
            Bson::Double(f64::NAN),
            Bson::Int32(1),
            Bson::Double(1.0),
            Bson::Int64(1),
            Bson::Double(-0.0),
            Bson::Double(0.0),
            Bson::Null,
            Bson::Undefined,
            Bson::Document(doc! { "a": 1, "b": 2 }),
            Bson::Document(doc! { "b": 2, "a": 1 }),
            Bson::Document(doc! { "a": 1.0, "b": 2 }),
            Bson::String("s".into()),
            Bson::Symbol("s".into()),
            Bson::Array(vec![Bson::Int32(1)]),
            Bson::Array(vec![Bson::Double(1.0)]),
            Bson::Boolean(true),
        ]
    }

    #[test]
    fn a_member_is_what_eq_calls_equal_and_agrees_with_add_to_set() {
        let corpus = corpus();
        let got = on(doc! { "$setUnion": ["$v"] }, doc! { "v": corpus.clone() });
        let Bson::Array(got) = got else { panic!("{got:?}") };
        // NaN, 1, 0, null, two documents, the string, [1] and true: nine.
        assert_eq!(got.len(), 9, "{got:?}");
        // Pairwise, no two members are `$eq`, and every corpus value is `$eq`
        // to exactly one member, which is its first appearance.
        for (i, x) in got.iter().enumerate() {
            for y in &got[i + 1..] {
                assert_ne!(canonical_cmp(x, y), Ordering::Equal, "{x:?} {y:?}");
            }
        }
        for v in &corpus {
            let same: Vec<_> =
                got.iter().filter(|m| canonical_cmp(m, v) == Ordering::Equal).collect();
            assert_eq!(same.len(), 1, "{v:?}");
            let first = corpus.iter().find(|c| canonical_cmp(c, v) == Ordering::Equal).unwrap();
            assert_eq!(format!("{:?}", same[0]), format!("{first:?}"));
        }
        // The members `$addToSet` keeps (ADR-186) are the same ones.
        let keys = |values: &[Bson]| -> std::collections::BTreeSet<Vec<u8>> {
            values.iter().map(crate::aggregate::group_key).collect()
        };
        assert_eq!(keys(&got), keys(&corpus));
        assert_eq!(keys(&got).len(), got.len());
        // And they do not depend on the order the values arrive in.
        for shift in 1..corpus.len() {
            let mut rotated = corpus.clone();
            rotated.rotate_left(shift);
            let again = on(doc! { "$setUnion": ["$v"] }, doc! { "v": rotated });
            let Bson::Array(again) = again else { panic!() };
            assert_eq!(keys(&again), keys(&got), "rotation {shift}");
        }
    }

    fn dec(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap())
    }

    #[test]
    fn a_decimal128_anywhere_in_a_set_input_is_refused() {
        // The canonical order ranks a Decimal128 equal to every number, so
        // `[Decimal128("5")]` would be a subset of `[1]`.
        for op in ["$setUnion", "$setIntersection", "$setDifference", "$setEquals", "$setIsSubset"]
        {
            for (d, at) in [
                (doc! { "a": [1], "b": [dec("5")] }, "argument 2"),
                (doc! { "a": [{ "n": dec("5") }], "b": [1] }, "argument 1"),
                (doc! { "a": [1], "b": [[2, dec("5")]] }, "argument 2"),
            ] {
                let mut expr = Document::new();
                expr.insert(op, vec![Bson::from("$a"), Bson::from("$b")]);
                let msg = refused(expr, d.clone());
                assert!(
                    msg.contains(op) && msg.contains("Decimal128") && msg.contains(at),
                    "{op} {d:?}: {msg}"
                );
            }
            // Beside a null, nothing is compared and the result is null.
            let mut expr = Document::new();
            expr.insert(op, vec![Bson::from("$b"), Bson::Null]);
            assert_eq!(on(expr, doc! { "b": [dec("5")] }), Bson::Null, "{op}");
        }
    }

    #[test]
    fn any_and_all_read_each_element_by_truthiness() {
        let d = doc! {
            "falsy": [0, false, null, 0.0, Bson::Undefined],
            "truthy": [1, "", [], {}, -0.5],
            "mixed": [0, 1],
            "nested": [[false], [0]],
            "empty": [],
        };
        let any = |f: &str| on(doc! { "$anyElementTrue": [format!("${f}")] }, d.clone());
        let all = |f: &str| on(doc! { "$allElementsTrue": [format!("${f}")] }, d.clone());
        assert_eq!(any("falsy"), false.into());
        assert_eq!(all("falsy"), false.into());
        assert_eq!(any("truthy"), true.into());
        assert_eq!(all("truthy"), true.into());
        assert_eq!(any("mixed"), true.into());
        assert_eq!(all("mixed"), false.into());
        // An array element is true, whatever it holds: nothing is descended.
        assert_eq!(all("nested"), true.into());
        // Nothing true and nothing false.
        assert_eq!(any("empty"), false.into());
        assert_eq!(all("empty"), true.into());
        // The shorthand takes the field directly.
        assert_eq!(on(doc! { "$anyElementTrue": "$mixed" }, d.clone()), true.into());
        assert_eq!(ok(doc! { "$allElementsTrue": [[true, 1]] }), true.into());
        for op in ["$anyElementTrue", "$allElementsTrue"] {
            let mut expr = Document::new();
            expr.insert(op, "$missing");
            assert_eq!(on(expr, d.clone()), Bson::Null, "{op}");
            let mut expr = Document::new();
            expr.insert(op, vec![Bson::Null]);
            assert_eq!(on(expr, d.clone()), Bson::Null, "{op}");
            let mut expr = Document::new();
            expr.insert(op, "$s");
            let msg = refused(expr, doc! { "s": "yes" });
            assert!(msg.contains(op) && msg.contains("found a string"), "{msg}");
        }
    }

    #[test]
    fn any_and_all_read_a_decimal128_element_by_its_value() {
        // A zero, of any sign or exponent, is false; any other value, `NaN`
        // included, is true. Nothing is compared, so unlike the set
        // operators there is nothing to refuse.
        for (items, any, all) in [
            (vec![dec("0")], false, false),
            (vec![dec("-0.000")], false, false),
            (vec![dec("0E-6176"), Bson::Int32(0)], false, false),
            (vec![dec("1")], true, true),
            (vec![dec("NaN"), dec("-Infinity")], true, true),
            (vec![Bson::Int32(1), dec("0")], true, false),
            (vec![dec("0"), Bson::Int32(1)], true, false),
            (vec![Bson::Int32(0), dec("1")], true, false),
        ] {
            let d = doc! { "v": items.clone() };
            assert_eq!(on(doc! { "$anyElementTrue": "$v" }, d.clone()), any.into(), "{items:?}");
            assert_eq!(on(doc! { "$allElementsTrue": "$v" }, d), all.into(), "{items:?}");
        }
        // Nested inside an element, it is not read: the element is an array.
        assert_eq!(
            on(doc! { "$allElementsTrue": "$v" }, doc! { "v": [[dec("0")]] }),
            Bson::Boolean(true)
        );
    }

    #[test]
    fn object_to_array_follows_the_documents_field_order() {
        let d = doc! { "o": { "b": 1, "a": { "x": 2 }, "c": [1, 2], "n": null } };
        let got = on(doc! { "$objectToArray": "$o" }, d);
        exactly(
            &got,
            &[
                Bson::Document(doc! { "k": "b", "v": 1 }),
                Bson::Document(doc! { "k": "a", "v": { "x": 2 } }),
                Bson::Document(doc! { "k": "c", "v": [1, 2] }),
                Bson::Document(doc! { "k": "n", "v": null }),
            ],
        );
        exactly(&on(doc! { "$objectToArray": "$o" }, doc! { "o": {} }), &[]);
        assert_eq!(on(doc! { "$objectToArray": "$missing" }, doc! {}), Bson::Null);
        assert_eq!(ok(doc! { "$objectToArray": [null] }), Bson::Null);
        for value in [Bson::from("text"), Bson::Array(vec![]), Bson::Int32(3)] {
            let msg = refused(doc! { "$objectToArray": "$o" }, doc! { "o": value.clone() });
            assert!(msg.contains("$objectToArray needs a document"), "{value:?}: {msg}");
        }
    }

    #[test]
    fn array_to_object_reads_both_forms() {
        let pairs =
            on(doc! { "$arrayToObject": "$p" }, doc! { "p": [["b", 1], ["a", { "x": 2 }]] });
        assert_eq!(
            format!("{pairs:?}"),
            format!("{:?}", Bson::Document(doc! { "b": 1, "a": { "x": 2 } }))
        );
        let kv = on(
            doc! { "$arrayToObject": "$p" },
            doc! { "p": [{ "k": "b", "v": 1 }, { "v": [2], "k": "a" }] },
        );
        assert_eq!(format!("{kv:?}"), format!("{:?}", Bson::Document(doc! { "b": 1, "a": [2] })));
        assert_eq!(on(doc! { "$arrayToObject": "$p" }, doc! { "p": [] }), Bson::Document(doc! {}));
        assert_eq!(on(doc! { "$arrayToObject": "$missing" }, doc! {}), Bson::Null);
        assert_eq!(ok(doc! { "$arrayToObject": [null] }), Bson::Null);
        let msg = refused(doc! { "$arrayToObject": "$p" }, doc! { "p": { "a": 1 } });
        assert!(msg.contains("$arrayToObject needs an array, found a document"), "{msg}");
    }

    #[test]
    fn a_repeated_key_keeps_its_first_place_and_its_last_value() {
        for p in [
            Bson::Array(vec![
                Bson::Array(vec!["a".into(), 1.into()]),
                Bson::Array(vec!["b".into(), 2.into()]),
                Bson::Array(vec!["a".into(), 3.into()]),
            ]),
            Bson::Array(vec![
                Bson::Document(doc! { "k": "a", "v": 1 }),
                Bson::Document(doc! { "k": "b", "v": 2 }),
                Bson::Document(doc! { "k": "a", "v": 3 }),
            ]),
        ] {
            let got = on(doc! { "$arrayToObject": "$p" }, doc! { "p": p });
            assert_eq!(
                format!("{got:?}"),
                format!("{:?}", Bson::Document(doc! { "a": 3, "b": 2 }))
            );
        }
    }

    #[test]
    fn array_to_object_refuses_a_malformed_pair_naming_the_element() {
        let cases: Vec<(Bson, &str)> = vec![
            (
                Bson::Array(vec![Bson::Array(vec!["a".into()])]),
                "element 0 is an array of 1 elements",
            ),
            (
                Bson::Array(vec![
                    Bson::Array(vec!["a".into(), 1.into()]),
                    Bson::Array(vec!["b".into(), 2.into(), 3.into()]),
                ]),
                "element 1 is an array of 3 elements",
            ),
            (
                Bson::Array(vec![Bson::Document(doc! { "k": "a", "v": 1, "w": 2 })]),
                "element 0 has a field \"w\" besides `k` and `v`",
            ),
            (
                Bson::Array(vec![Bson::Document(doc! { "k": "a" })]),
                "element 0 needs both `k` and `v`",
            ),
            (Bson::Array(vec![Bson::Document(doc! {})]), "element 0 needs both `k` and `v`"),
            (
                Bson::Array(vec![
                    Bson::Array(vec!["a".into(), 1.into()]),
                    Bson::Document(doc! { "k": "b", "v": 2 }),
                ]),
                "cannot mix the two forms: element 0 is a [key, value] array, element 1 is a {k, v} document",
            ),
            (
                Bson::Array(vec![
                    Bson::Document(doc! { "k": "b", "v": 2 }),
                    Bson::Array(vec!["a".into(), 1.into()]),
                ]),
                "element 0 is a {k, v} document, element 1 is a [key, value] array",
            ),
            (
                Bson::Array(vec![Bson::Array(vec![1.into(), 1.into()])]),
                "has a key that is an integer",
            ),
            (
                Bson::Array(vec![Bson::Document(doc! { "k": null, "v": 1 })]),
                "has a key that is null",
            ),
            (Bson::Array(vec!["a".into()]), "element 0 is a string"),
            (Bson::Array(vec![Bson::Null]), "element 0 is null"),
        ];
        for (p, want) in cases {
            let msg = refused(doc! { "$arrayToObject": "$p" }, doc! { "p": p.clone() });
            assert!(msg.contains(want), "{p:?}: {msg}");
        }
    }

    #[test]
    fn array_to_object_refuses_only_a_key_holding_a_nul() {
        for key in ["a\0b", "\0"] {
            for p in [
                Bson::Array(vec![
                    Bson::Array(vec!["ok".into(), 0.into()]),
                    Bson::Array(vec![key.into(), 1.into()]),
                ]),
                Bson::Array(vec![
                    Bson::Document(doc! { "k": "ok", "v": 0 }),
                    Bson::Document(doc! { "k": key, "v": 1 }),
                ]),
            ] {
                let msg = refused(doc! { "$arrayToObject": "$p" }, doc! { "p": p.clone() });
                assert!(msg.contains("element 1") && msg.contains("NUL"), "{key:?}: {msg}");
            }
        }
        // Every other string is a key, taken as written, in both forms.
        for key in ["", "$x", "a.b", ".", "a$"] {
            let mut want = Document::new();
            want.insert(key, 1);
            for p in [
                Bson::Array(vec![Bson::Array(vec![key.into(), 1.into()])]),
                Bson::Array(vec![Bson::Document(doc! { "k": key, "v": 1 })]),
            ] {
                let got = on(doc! { "$arrayToObject": "$p" }, doc! { "p": p });
                assert_eq!(got, Bson::Document(want.clone()), "{key:?}");
            }
        }
    }

    #[test]
    fn a_stored_document_with_any_keys_round_trips_through_pairs() {
        // Storage, an object expression and `$literal` all take these keys,
        // so turning such a document into pairs and back must not refuse it.
        let mut o = Document::new();
        o.insert("a.b", 1);
        o.insert("$x", "dollar");
        o.insert("", doc! { "inner.dot": 2 });
        o.insert("plain", [1, 2]);
        let got =
            on(doc! { "$arrayToObject": { "$objectToArray": "$o" } }, doc! { "o": o.clone() });
        assert_eq!(format!("{got:?}"), format!("{:?}", Bson::Document(o)));
    }

    #[test]
    fn object_to_array_and_back_is_the_same_document() {
        let o = doc! { "z": 1_i64, "a": [1, { "b": 2.5 }], "m": { "n": null }, "s": "t" };
        let got =
            on(doc! { "$arrayToObject": { "$objectToArray": "$o" } }, doc! { "o": o.clone() });
        assert_eq!(format!("{got:?}"), format!("{:?}", Bson::Document(o)));
    }

    #[test]
    fn the_documented_rename_through_pairs_works() {
        // The example in aggregation.md, "Documents as pairs".
        let expr = doc! { "$arrayToObject": { "$map": {
            "input": { "$objectToArray": "$attrs" },
            "in": { "k": { "$toUpper": "$$this.k" }, "v": "$$this.v" },
        } } };
        let got = on(expr, doc! { "attrs": { "tier": "gold", "since": 2019 } });
        assert_eq!(got, Bson::Document(doc! { "TIER": "gold", "SINCE": 2019 }));
    }

    #[test]
    fn zip_and_sort_array_stay_unknown_operators() {
        for name in ["$zip", "$sortArray"] {
            let mut expr = Document::new();
            expr.insert(name, doc! { "inputs": [[1]] });
            let msg = Expr::parse(&Bson::Document(expr)).unwrap_err().to_string();
            assert!(msg.contains(name) && msg.contains("not an expression operator"), "{msg}");
        }
    }
}

/// An array written inside an expression is an array of expressions, read in
/// the scope it sits in (ADR-215).
#[cfg(test)]
mod arrays {
    use super::*;
    use bson::{bson, doc};
    use proptest::prelude::*;

    fn row() -> Document {
        doc! {
            "a": 1, "b": 2, "tags": ["x"], "extra": "y", "xs": [1, 2],
            "s": "text", "n": 0,
        }
    }

    fn ev(expr: Bson) -> Result<Bson> {
        Expr::parse(&expr)?.eval(&row())
    }

    fn ok(expr: Bson) -> Bson {
        ev(expr.clone()).unwrap_or_else(|e| panic!("{expr}: {e}"))
    }

    #[test]
    fn a_field_path_in_an_array_is_read() {
        assert_eq!(ok(bson!(["$a", "$b"])), bson!([1, 2]));
        // The worked example: a nested operand is read too.
        assert_eq!(ok(bson!({"$setUnion": ["$tags", ["$extra"]]})), bson!(["x", "y"]));
        assert_eq!(ok(bson!({"$in": [2, ["$a", "$b"]]})), bson!(true));
        assert_eq!(ok(bson!({"$in": ["$b", ["$a", 3]]})), bson!(false));
        assert_eq!(
            ok(bson!({"$concatArrays": ["$xs", ["$a", {"$add": ["$b", 1]}]]})),
            bson!([1, 2, 1, 3_i64])
        );
        assert_eq!(ok(bson!({"$size": [["$a", "$b", "$s"]]})), bson!(3_i64));
        // An operator, a document and a variable as elements.
        assert_eq!(
            ok(bson!([{"$add": ["$a", "$b"]}, {"k": "$s"}, "$$ROOT.a", "plain"])),
            bson!([3_i64, {"k": "text"}, 1, "plain"])
        );
    }

    #[test]
    fn arrays_nest_and_documents_inside_them_are_read() {
        assert_eq!(ok(bson!([["$a", ["$b"]], 3])), bson!([[1, [2]], 3]));
        assert_eq!(ok(bson!([{"p": ["$a", {"q": ["$b"]}]}])), bson!([{"p": [1, {"q": [2]}]}]));
        assert_eq!(ok(bson!({"o": [{"v": "$s"}]})), bson!({"o": [{"v": "text"}]}));
    }

    #[test]
    fn a_missing_field_is_null_in_its_place() {
        assert_eq!(ok(bson!(["$a", "$missing", "$b"])), bson!([1, null, 2]));
        assert_eq!(ok(bson!({"$concatArrays": [["$missing"], [1]]})), bson!([null, 1]));
        assert_eq!(ok(bson!({"$size": [["$missing", "$nope"]]})), bson!(2_i64));
        assert_eq!(ok(bson!({"$arrayElemAt": [["$missing", "$b"], 1]})), bson!(2));
        // An array holding a null is an array, not a null: the operators that
        // propagate a null argument do not see one.
        assert_eq!(ok(bson!({"$setUnion": [["$missing"], [1]]})), bson!([null, 1]));
    }

    #[test]
    fn a_literal_keeps_an_array_as_written() {
        assert_eq!(
            ok(bson!({"$literal": ["$a", "$$x", {"$add": [1]}]})),
            bson!(["$a", "$$x", {"$add": [1]}])
        );
        assert_eq!(ok(bson!({"$in": ["$a", {"$literal": ["$a"]}]})), bson!(false));
        assert_eq!(ok(bson!({"$in": ["$a", {"$literal": [1]}]})), bson!(true));
    }

    #[test]
    fn an_element_reads_the_variables_of_the_scope_it_sits_in() {
        let mapped =
            bson!({"$map": {"input": "$xs", "as": "x", "in": ["$$x", {"$multiply": ["$$x", 10]}]}});
        assert_eq!(ok(mapped), bson!([[1, 10_i64], [2, 20_i64]]));
        let filtered = bson!({"$filter": {"input": "$xs", "cond": {"$in": ["$$this", ["$a", 5]]}}});
        assert_eq!(ok(filtered), bson!([1]));
        let reduced = bson!({"$reduce": {
            "input": "$xs", "initialValue": [], "in": {"$concatArrays": ["$$value", ["$$this"]]}
        }});
        assert_eq!(ok(reduced), bson!([1, 2]));
        let bound = bson!({"$let": {"vars": {"v": "$b"}, "in": [["$$v"]]}});
        assert_eq!(ok(bound), bson!([[2]]));
        // A `$lookup` let name, bound by the caller.
        let parsed = Expr::parse_with_vars(&bson!(["$$oid", "$a"]), &["oid".to_string()]).unwrap();
        let oid = Bson::Int32(7);
        let frame = [("oid", &oid)];
        assert_eq!(parsed.eval_in(&Scope::with_bindings(&row(), &frame)).unwrap(), bson!([7, 1]));
        // A name nothing binds is refused at parse, inside an array as anywhere.
        let msg = Expr::parse(&bson!({"$in": [1, ["$$nope"]]})).unwrap_err().to_string();
        assert!(msg.contains("unknown variable $$nope"), "{msg}");
        // And one bound around the array but not in scope outside it is free.
        let msg = Expr::parse(&bson!([{"$let": {"vars": {"v": 1}, "in": "$$v"}}, "$$v"]))
            .unwrap_err()
            .to_string();
        assert!(msg.contains("unknown variable $$v"), "{msg}");
    }

    #[test]
    fn free_variables_are_collected_inside_an_array() {
        assert_eq!(
            Expr::free_variables(&bson!({"$in": ["gasket", ["$$line.sku", "$$line.alt"]]}))
                .unwrap(),
            vec!["line".to_string()]
        );
        assert_eq!(
            Expr::free_variables(&bson!([["$$a"], {"k": ["$$b"]}])).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        // A `$literal` array names nothing.
        assert!(Expr::free_variables(&bson!({"$literal": ["$$line"]})).unwrap().is_empty());
    }

    #[test]
    fn an_array_reads_the_document_when_an_element_does() {
        let reads =
            |v: Bson| Expr::parse_with_vars(&v, &["line".to_string()]).unwrap().reads_document();
        assert!(reads(bson!({"$in": ["$$line", ["$a"]]})));
        assert!(reads(bson!([[{"k": "$$ROOT"}]])));
        assert!(!reads(bson!({"$in": ["$$line", ["$$line.alt", 1]]})));
        // One element that reads is enough, wherever it sits.
        assert!(reads(bson!({"$in": ["$$line", [1, "$a"]]})));
        assert!(reads(bson!([["$$line.alt", 1], ["$$line.alt", "$$ROOT"]])));
        assert!(!reads(bson!([1, [2]])));
    }

    #[test]
    fn an_element_that_fails_fails_the_array_with_its_own_error() {
        let msg = ev(bson!(["$a", {"$add": ["$s", 1]}])).unwrap_err().to_string();
        assert!(msg.contains("$add") && msg.contains("string"), "{msg}");
        // The first failing element in order is the one reported.
        let msg = ev(bson!([{"$divide": [1, "$n"]}, {"$add": ["$s", 1]}])).unwrap_err().to_string();
        assert!(msg.contains("$divide"), "{msg}");
    }

    #[test]
    fn a_failing_array_is_held_by_and_and_or_like_any_value_error() {
        // ADR-211: a value error waits while another argument may decide.
        let bad = bson!([{"$add": ["$s", 1]}]);
        let sized = bson!({"$gt": [{"$size": [bad.clone()]}, 0]});
        for (expr, want) in [
            (bson!({"$and": [bad.clone(), false]}), false),
            (bson!({"$and": [false, bad.clone()]}), false),
            (bson!({"$or": [bad.clone(), true]}), true),
            (bson!({"$or": [true, sized.clone()]}), true),
            (bson!({"$and": [sized.clone(), "$missing"]}), false),
        ] {
            assert_eq!(ev(expr.clone()).unwrap(), Bson::Boolean(want), "{expr}");
        }
        // Undecided, the array's own error stands.
        for expr in [bson!({"$and": [bad.clone(), true]}), bson!({"$or": [false, sized]})] {
            let msg = ev(expr.clone()).unwrap_err().to_string();
            assert!(msg.contains("$add"), "{expr}: {msg}");
        }
    }

    #[test]
    fn an_array_of_literals_is_folded_to_one_literal() {
        assert_eq!(
            Expr::parse(&bson!([1, ["two", null], true])).unwrap(),
            Expr::Literal(bson!([1, ["two", null], true]))
        );
        assert_eq!(Expr::parse(&bson!([])).unwrap(), Expr::Literal(bson!([])));
        // One element to read and the array is not folded, at any depth.
        assert!(matches!(Expr::parse(&bson!([1, ["$a"]])).unwrap(), Expr::Array(_)));
        assert!(matches!(Expr::parse(&bson!([1, {"k": "$a"}])).unwrap(), Expr::Array(_)));
        assert!(matches!(Expr::parse(&bson!([[{"k": [1, "$a"]}]])).unwrap(), Expr::Array(_)));
        // A Decimal128 element is refused, folded or not.
        let d = Bson::Decimal128("1.5".parse().unwrap());
        for array in [Bson::Array(vec![d.clone()]), Bson::Array(vec![Bson::from("$a"), d])] {
            let msg = Expr::parse(&array).unwrap_err().to_string();
            assert!(msg.contains("Decimal128 literal"), "{msg}");
        }
    }

    /// An array of constants folds to one literal even when an element is a
    /// document of constants, at any depth, so no walk is made per document.
    #[test]
    fn a_constant_nested_array_or_document_folds_to_one_literal() {
        let folded = |v: Bson| Expr::parse(&v).unwrap();
        for value in [
            bson!([{"a": 1}]),
            bson!([{"a": [1, {"b": [2, {}]}], "c": "text"}, [{"d": null}], {}]),
            bson!([[[{"k": [[{"j": 1}]]}]]]),
        ] {
            assert_eq!(folded(value.clone()), Expr::Literal(value.clone()), "{value}");
        }
        // Inside an operand the whole nested array is one literal node.
        let value = bson!([{"a": [1, {"b": 2}]}, [3]]);
        assert_eq!(
            folded(bson!({"$size": [value.clone()]})),
            Expr::Op(Op::Size, vec![Expr::Literal(value)])
        );
        // One path anywhere keeps the array to walk, and only it: the
        // constants around it are folded where they stand.
        let Expr::Array(items) = folded(bson!([{"a": 1}, [2, {"b": 3}], {"c": "$a"}])) else {
            panic!("an element reads the document");
        };
        assert_eq!(items[0], Expr::Literal(bson!({"a": 1})));
        assert_eq!(items[1], Expr::Literal(bson!([2, {"b": 3}])));
        assert!(matches!(items[2], Expr::Object(_)));
        // The answer is the same either way.
        assert_eq!(
            ok(bson!([{"a": 1}, [2, {"b": 3}], {"c": "$a"}])),
            bson!([{"a": 1}, [2, {"b": 3}], {"c": 1}])
        );
    }

    /// A document written in an array is read as an expression, so one with a
    /// `$`-prefixed key is an operator and can be refused. `$literal` keeps it.
    #[test]
    fn an_operator_shaped_document_in_an_array_is_parsed_and_literal_keeps_it() {
        let msg = Expr::parse(&bson!([{"$gt": 1}])).unwrap_err().to_string();
        assert!(msg.contains("$gt takes exactly 2 argument(s)"), "{msg}");
        let msg = Expr::parse(&bson!({"$in": ["$a", [{"$gt": 1}]]})).unwrap_err().to_string();
        assert!(msg.contains("$gt takes exactly 2 argument(s)"), "{msg}");
        let msg = Expr::parse(&bson!([{"a": 1, "$b": 2}])).unwrap_err().to_string();
        assert!(msg.contains("cannot mix operator"), "{msg}");
        // Written under `$literal`, the same documents are values.
        assert_eq!(ok(bson!({"$literal": [{"$gt": 1}]})), bson!([{"$gt": 1}]));
        assert_eq!(ok(bson!({"$literal": [{"a": 1, "$b": 2}]})), bson!([{"a": 1, "$b": 2}]));
        let kept = bson!({"$in": [{"$literal": {"$gt": 1}}, {"$literal": [{"$gt": 1}]}]});
        assert_eq!(ok(kept), bson!(true));
    }

    /// The named operands of `$reduce`, `$switch`, `$convert`, `$map`,
    /// `$filter` and `$let` are expressions like any other, so an array
    /// written as one is read, not kept as text.
    #[test]
    fn an_array_written_as_a_named_operand_is_read() {
        assert_eq!(
            ok(bson!({"$reduce": {
                "input": "$xs", "initialValue": ["$a"],
                "in": {"$concatArrays": ["$$value", ["$$this"]]}
            }})),
            bson!([1, 1, 2])
        );
        assert_eq!(
            ok(bson!({"$switch": {"branches": [{"case": true, "then": ["$a", "$b"]}]}})),
            bson!([1, 2])
        );
        assert_eq!(
            ok(bson!({"$switch": {
                "branches": [{"case": false, "then": 0}], "default": ["$a", "$b"]
            }})),
            bson!([1, 2])
        );
        assert_eq!(
            ok(bson!({"$convert": {"input": "$s", "to": "int", "onError": ["$a"]}})),
            bson!([1])
        );
        assert_eq!(
            ok(bson!({"$convert": {"input": "$missing", "to": "int", "onNull": ["$b"]}})),
            bson!([2])
        );
        assert_eq!(
            ok(bson!({"$map": {"input": ["$a", "$b"], "as": "x", "in": {"$add": ["$$x", 1]}}})),
            bson!([2_i64, 3_i64])
        );
        assert_eq!(
            ok(bson!({"$filter": {"input": ["$a", "$b"], "cond": {"$gt": ["$$this", 1]}}})),
            bson!([2])
        );
        assert_eq!(ok(bson!({"$let": {"vars": {"v": ["$a", "$b"]}, "in": "$$v"}})), bson!([1, 2]));
    }

    /// A value an expression reads as itself: no `$`-prefixed string or key,
    /// at any depth. Documents and nested arrays are included, so the folded
    /// shape of each is held equal to the unfolded one below.
    fn literal_value() -> impl Strategy<Value = Bson> {
        let leaf = prop_oneof![
            Just(Bson::Null),
            any::<bool>().prop_map(Bson::Boolean),
            any::<i32>().prop_map(Bson::Int32),
            any::<i64>().prop_map(Bson::Int64),
            (-1.0e6f64..1.0e6).prop_map(Bson::Double),
            "[a-z ]{0,6}".prop_map(Bson::String),
        ];
        leaf.prop_recursive(4, 32, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Bson::Array),
                prop::collection::vec(("[a-z]{1,4}", inner), 1..4)
                    .prop_map(|fields| { Bson::Document(fields.into_iter().collect()) }),
            ]
        })
    }

    proptest! {
        #[test]
        fn an_array_of_literals_evaluates_to_itself(
            items in prop::collection::vec(literal_value(), 0..8),
        ) {
            let array = Bson::Array(items.clone());
            let parsed = Expr::parse(&array).unwrap();
            prop_assert_eq!(parsed.eval(&row()).unwrap(), array.clone());
            // Folded or not, the same answer: each element parsed alone and
            // the array built from them.
            let unfolded =
                Expr::Array(items.iter().map(|v| Expr::parse(v).unwrap()).collect());
            prop_assert_eq!(unfolded.eval(&row()).unwrap(), array.clone());
            // And through an operator that takes it as one operand.
            let sized = Expr::parse(&bson!({"$size": [array.clone()]})).unwrap();
            prop_assert_eq!(sized.eval(&row()).unwrap(), Bson::Int64(items.len() as i64));
        }
    }
}
