//! `$jsonSchema`: a schema document parsed once and tested against documents.
//!
//! The dialect is draft-4 JSON Schema as the reference system reads it, with
//! `bsonType` beside `type`. Every keyword it knows is listed in [`Schema`];
//! any other keyword, wherever it is written in the schema, is refused when
//! the schema is parsed, so a schema that parses is a schema this module
//! evaluates in full. `$ref`, `format`, `default` and the rest are refused by
//! name rather than skipped: a validator that ignored a keyword would accept
//! documents its author meant to reject.
//!
//! Evaluation is pure. It allocates only for `uniqueItems` (a vector of
//! references to the array's elements, so at most 8 bytes per element and
//! bounded by the 16 MiB document cap) and, in
//! [`Schema::explain_failure`], for the message. Its recursion follows the
//! schema, never the document: each step into a property or an element moves
//! to a sub-schema, so it is at most [`MAX_DEPTH`] deep whatever the nesting
//! of the document tested. (Only the order of two values, in `enum` and
//! `uniqueItems`, descends into the document, as every comparison does.)

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bson::{Bson, Document};

use crate::cmp::canonical_cmp;
use crate::decimal::{self, Decimal};
use crate::regex::{RegexBudget, RegexPattern};
use crate::type_names::{TYPE_NAMES, type_name_of};
use crate::{Error, Result};

/// The deepest a schema may nest, the outermost schema being level 1: each
/// of `properties`, `patternProperties`, `items`, `additionalProperties`,
/// `additionalItems`, `dependencies`, `allOf`, `anyOf`, `oneOf` and `not`
/// goes one level down.
pub const MAX_DEPTH: usize = 32;

/// A parsed `$jsonSchema`.
///
/// Parsing checks every keyword and its argument, compiles every pattern and
/// bounds the nesting, so [`Schema::is_valid`] has nothing left to refuse.
/// Cloning shares the parsed form.
///
/// The keywords: `bsonType`, `type`, `required`, `properties`,
/// `additionalProperties`, `patternProperties`, `enum`, `minimum`, `maximum`,
/// `exclusiveMinimum`, `exclusiveMaximum`, `multipleOf`, `minLength`,
/// `maxLength`, `pattern`, `items`, `additionalItems`, `minItems`,
/// `maxItems`, `uniqueItems`, `minProperties`, `maxProperties`,
/// `dependencies`, `allOf`, `anyOf`, `oneOf`, `not`, and `title` and
/// `description`, which are read and ignored.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    source: Arc<Document>,
    root: Arc<Node>,
}

impl Schema {
    /// Parse a schema document.
    ///
    /// `Err` is an [`Error::InvalidQuery`], the `400` a malformed filter
    /// meets, naming the keyword and where in the schema it stands. Every
    /// `pattern` is compiled as a `$regex` is and charged to the same budget:
    /// inside a filter, the filter's; otherwise one of its own, so a schema
    /// holds at most [`crate::regex::REGEX_CONDITIONS_LIMIT`] patterns.
    pub fn parse(schema: &Bson) -> Result<Schema> {
        let _budget = RegexBudget::open();
        let Bson::Document(source) = schema else {
            return Err(Error::InvalidQuery("$jsonSchema requires a schema document".into()));
        };
        let root = Parser { path: Vec::new() }.node(schema, 1)?;
        Ok(Schema { source: Arc::new(source.clone()), root: Arc::new(root) })
    }

    /// The schema document this was parsed from.
    pub fn document(&self) -> &Document {
        &self.source
    }

    /// Whether `doc` satisfies the schema.
    pub fn is_valid(&self, doc: &Document) -> bool {
        Run::new(false).node(&self.root, V::Doc(doc), 1)
    }

    /// Why `doc` does not satisfy the schema, or `None` when it does.
    ///
    /// The reason is for the first keyword that fails, in a fixed order:
    /// `bsonType`, `type`, `enum`, `allOf`, `anyOf`, `oneOf`, `not`, then the
    /// keywords of the value's own type, an object's `required` first and its
    /// `properties` in the order the schema lists them. It reads
    /// `<where>: <what>`, `where` being the dotted path of the value in the
    /// document, or `(document)` for the document itself.
    pub fn explain_failure(&self, doc: &Document) -> Option<String> {
        let mut run = Run::new(true);
        if run.node(&self.root, V::Doc(doc), 1) {
            return None;
        }
        Some(run.reason.unwrap_or_else(|| "(document): does not satisfy the schema".into()))
    }
}

// ---------------------------------------------------------------------------
// The parsed form
// ---------------------------------------------------------------------------

#[derive(Debug, Default, PartialEq)]
struct Node {
    bson_type: Option<Vec<TypeSpec>>,
    json_type: Option<Vec<TypeSpec>>,
    enum_values: Option<Vec<Bson>>,
    all_of: Vec<Node>,
    any_of: Vec<Node>,
    one_of: Vec<Node>,
    not: Option<Box<Node>>,

    minimum: Option<Bound>,
    maximum: Option<Bound>,
    multiple_of: Option<Bson>,

    min_length: Option<usize>,
    max_length: Option<usize>,
    pattern: Option<RegexPattern>,

    required: Vec<String>,
    properties: Vec<(String, Node)>,
    /// Where each name of `properties` stands, for the keys of a document.
    property_at: HashMap<String, usize>,
    additional_properties: Additional,
    pattern_properties: Vec<(RegexPattern, Node)>,
    min_properties: Option<usize>,
    max_properties: Option<usize>,
    dependencies: Vec<(String, Dependency)>,

    items: Items,
    additional_items: Additional,
    min_items: Option<usize>,
    max_items: Option<usize>,
    unique_items: bool,
}

/// `minimum` or `maximum`, with the draft-4 flag that makes it exclusive.
#[derive(Debug, PartialEq)]
struct Bound {
    value: Bson,
    exclusive: bool,
}

#[derive(Debug, Default, PartialEq)]
enum Additional {
    #[default]
    Allowed,
    Denied,
    Schema(Box<Node>),
}

#[derive(Debug, Default, PartialEq)]
enum Items {
    #[default]
    Unset,
    All(Box<Node>),
    Each(Vec<Node>),
}

#[derive(Debug, PartialEq)]
enum Dependency {
    Properties(Vec<String>),
    Schema(Box<Node>),
}

/// One name of `type` or `bsonType`, and what it accepts.
#[derive(Clone, Copy, Debug, PartialEq)]
struct TypeSpec {
    label: &'static str,
    test: TypeTest,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TypeTest {
    /// `number`: an `int`, a `long`, a `double` or a `decimal`.
    Number,
    /// The name [`type_name_of`] gives a value of exactly this type.
    Named(&'static str),
}

impl TypeSpec {
    fn holds(self, v: V<'_>) -> bool {
        match self.test {
            TypeTest::Number => matches!(v, V::Val(b) if is_number(b)),
            TypeTest::Named(name) => v.type_name() == name,
        }
    }
}

/// The names `type` accepts, and the type each stands for.
const JSON_TYPES: [TypeSpec; 6] = [
    TypeSpec { label: "object", test: TypeTest::Named("object") },
    TypeSpec { label: "array", test: TypeTest::Named("array") },
    TypeSpec { label: "number", test: TypeTest::Number },
    TypeSpec { label: "boolean", test: TypeTest::Named("bool") },
    TypeSpec { label: "string", test: TypeTest::Named("string") },
    TypeSpec { label: "null", test: TypeTest::Named("null") },
];

fn is_number(b: &Bson) -> bool {
    matches!(b, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))
}

fn is_nan(b: &Bson) -> bool {
    match b {
        Bson::Double(d) => d.is_nan(),
        Bson::Decimal128(d) => matches!(decimal::decode(d), Decimal::Nan),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

const KEYWORDS: &str = "bsonType, type, required, properties, additionalProperties, \
    patternProperties, enum, minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf, \
    minLength, maxLength, pattern, items, additionalItems, minItems, maxItems, uniqueItems, \
    minProperties, maxProperties, dependencies, allOf, anyOf, oneOf, not, title, description";

struct Parser {
    /// The keywords and names from the outermost schema to the one being read.
    path: Vec<String>,
}

impl Parser {
    fn at(&self) -> String {
        if self.path.is_empty() { String::new() } else { format!(" at {}", self.path.join(".")) }
    }

    fn refuse(&self, keyword: &str, why: &str) -> Error {
        Error::InvalidQuery(format!("$jsonSchema{}: keyword {keyword:?} {why}", self.at()))
    }

    /// The schema in `value`, one level below the one being read, reached
    /// through `segments`.
    fn child(&mut self, segments: &[&str], value: &Bson, depth: usize) -> Result<Node> {
        self.path.extend(segments.iter().map(|s| (*s).to_string()));
        let node = self.node(value, depth + 1);
        self.path.truncate(self.path.len() - segments.len());
        node
    }

    fn node(&mut self, schema: &Bson, depth: usize) -> Result<Node> {
        let Bson::Document(doc) = schema else {
            return Err(Error::InvalidQuery(format!(
                "$jsonSchema{}: a schema must be a document",
                self.at()
            )));
        };
        if depth > MAX_DEPTH {
            return Err(Error::InvalidQuery(format!(
                "$jsonSchema{}: schemas nest at most {MAX_DEPTH} levels deep",
                self.at()
            )));
        }
        let mut node = Node::default();
        let mut exclusive_minimum = None;
        let mut exclusive_maximum = None;
        let mut minimum = None;
        let mut maximum = None;

        for (keyword, value) in doc {
            let kw = keyword.as_str();
            match kw {
                "bsonType" => node.bson_type = Some(self.types(kw, value, false)?),
                "type" => node.json_type = Some(self.types(kw, value, true)?),
                "enum" => match value {
                    Bson::Array(items) if !items.is_empty() => {
                        node.enum_values = Some(items.clone());
                    }
                    _ => return Err(self.refuse(kw, "must be a non-empty array")),
                },
                "allOf" => node.all_of = self.schemas(kw, value, depth)?,
                "anyOf" => node.any_of = self.schemas(kw, value, depth)?,
                "oneOf" => node.one_of = self.schemas(kw, value, depth)?,
                "not" => node.not = Some(Box::new(self.child(&[kw], value, depth)?)),

                "minimum" => minimum = Some(self.number(kw, value)?),
                "maximum" => maximum = Some(self.number(kw, value)?),
                "exclusiveMinimum" => exclusive_minimum = Some(self.flag(kw, value)?),
                "exclusiveMaximum" => exclusive_maximum = Some(self.flag(kw, value)?),
                "multipleOf" => node.multiple_of = Some(self.divisor(kw, value)?),

                "minLength" => node.min_length = Some(self.count(kw, value)?),
                "maxLength" => node.max_length = Some(self.count(kw, value)?),
                "pattern" => match value {
                    Bson::String(p) => node.pattern = Some(self.regex(kw, p)?),
                    _ => return Err(self.refuse(kw, "must be a string")),
                },

                "required" => node.required = self.names(kw, value)?,
                "properties" => {
                    let Bson::Document(props) = value else {
                        return Err(self.refuse(kw, "must be a document of schemas"));
                    };
                    for (name, sub) in props {
                        let parsed = self.child(&[kw, name], sub, depth)?;
                        node.property_at.insert(name.clone(), node.properties.len());
                        node.properties.push((name.clone(), parsed));
                    }
                }
                "additionalProperties" => {
                    node.additional_properties = self.additional(kw, value, depth)?;
                }
                "patternProperties" => {
                    let Bson::Document(props) = value else {
                        return Err(self.refuse(kw, "must be a document of schemas"));
                    };
                    for (pattern, sub) in props {
                        let regex = self.regex(kw, pattern)?;
                        node.pattern_properties
                            .push((regex, self.child(&[kw, pattern], sub, depth)?));
                    }
                }
                "minProperties" => node.min_properties = Some(self.count(kw, value)?),
                "maxProperties" => node.max_properties = Some(self.count(kw, value)?),
                "dependencies" => {
                    let Bson::Document(deps) = value else {
                        return Err(self.refuse(kw, "must be a document"));
                    };
                    for (name, dep) in deps {
                        let parsed = match dep {
                            Bson::Array(_) => Dependency::Properties(self.names(kw, dep)?),
                            Bson::Document(_) => {
                                Dependency::Schema(Box::new(self.child(&[kw, name], dep, depth)?))
                            }
                            _ => {
                                return Err(self.refuse(
                                    kw,
                                    "must map each name to an array of names or a schema",
                                ));
                            }
                        };
                        node.dependencies.push((name.clone(), parsed));
                    }
                }

                "items" => {
                    node.items = match value {
                        Bson::Document(_) => {
                            Items::All(Box::new(self.child(&[kw], value, depth)?))
                        }
                        Bson::Array(entries) => {
                            let mut each = Vec::with_capacity(entries.len());
                            for (at, entry) in entries.iter().enumerate() {
                                each.push(self.child(&[kw, &at.to_string()], entry, depth)?);
                            }
                            Items::Each(each)
                        }
                        _ => {
                            return Err(self.refuse(kw, "must be a schema or an array of schemas"));
                        }
                    };
                }
                "additionalItems" => node.additional_items = self.additional(kw, value, depth)?,
                "minItems" => node.min_items = Some(self.count(kw, value)?),
                "maxItems" => node.max_items = Some(self.count(kw, value)?),
                "uniqueItems" => node.unique_items = self.flag(kw, value)?,

                "title" | "description" => {
                    if !matches!(value, Bson::String(_)) {
                        return Err(self.refuse(kw, "must be a string"));
                    }
                }
                other => {
                    return Err(self.refuse(
                        other,
                        &format!("is not supported; the supported keywords are {KEYWORDS}"),
                    ));
                }
            }
        }

        node.minimum = self.bound("minimum", minimum, "exclusiveMinimum", exclusive_minimum)?;
        node.maximum = self.bound("maximum", maximum, "exclusiveMaximum", exclusive_maximum)?;
        Ok(node)
    }

    /// `minimum` or `maximum` with its exclusive flag. A flag with no bound
    /// to apply to is an error, as it is in the reference.
    fn bound(
        &self,
        keyword: &str,
        value: Option<Bson>,
        flag: &str,
        exclusive: Option<bool>,
    ) -> Result<Option<Bound>> {
        match (value, exclusive) {
            (Some(value), exclusive) => {
                Ok(Some(Bound { value, exclusive: exclusive.unwrap_or(false) }))
            }
            (None, Some(_)) => Err(self.refuse(flag, &format!("requires {keyword:?} beside it"))),
            (None, None) => Ok(None),
        }
    }

    /// `type` (`json`) or `bsonType`: a name or a non-empty array of names.
    fn types(&self, keyword: &str, value: &Bson, json: bool) -> Result<Vec<TypeSpec>> {
        let one = |name: &Bson| -> Result<TypeSpec> {
            let Bson::String(name) = name else {
                return Err(self.refuse(keyword, "must be a type name or an array of type names"));
            };
            let found = if json {
                JSON_TYPES.iter().find(|t| t.label == name).copied()
            } else if name == "number" {
                Some(JSON_TYPES[2])
            } else {
                TYPE_NAMES
                    .iter()
                    .find(|t| **t == name)
                    .map(|t| TypeSpec { label: t, test: TypeTest::Named(t) })
            };
            found.ok_or_else(|| {
                let known: Vec<&str> = if json {
                    JSON_TYPES.iter().map(|t| t.label).collect()
                } else {
                    std::iter::once("number").chain(TYPE_NAMES.iter().copied()).collect()
                };
                self.refuse(
                    keyword,
                    &format!("names no type: {name:?}; the names are {}", known.join(", ")),
                )
            })
        };
        match value {
            Bson::Array(names) if names.is_empty() => {
                Err(self.refuse(keyword, "must name at least one type"))
            }
            Bson::Array(names) => names.iter().map(one).collect(),
            single => Ok(vec![one(single)?]),
        }
    }

    /// `allOf`, `anyOf` or `oneOf`: a non-empty array of schemas.
    fn schemas(&mut self, keyword: &str, value: &Bson, depth: usize) -> Result<Vec<Node>> {
        match value {
            Bson::Array(entries) if !entries.is_empty() => entries
                .iter()
                .enumerate()
                .map(|(at, entry)| self.child(&[keyword, &at.to_string()], entry, depth))
                .collect(),
            _ => Err(self.refuse(keyword, "must be a non-empty array of schemas")),
        }
    }

    /// `additionalProperties` or `additionalItems`: a boolean or a schema.
    fn additional(&mut self, keyword: &str, value: &Bson, depth: usize) -> Result<Additional> {
        match value {
            Bson::Boolean(true) => Ok(Additional::Allowed),
            Bson::Boolean(false) => Ok(Additional::Denied),
            Bson::Document(_) => {
                Ok(Additional::Schema(Box::new(self.child(&[keyword], value, depth)?)))
            }
            _ => Err(self.refuse(keyword, "must be a boolean or a schema")),
        }
    }

    /// `required` or a property dependency: a non-empty array of distinct
    /// strings.
    fn names(&self, keyword: &str, value: &Bson) -> Result<Vec<String>> {
        let Bson::Array(entries) = value else {
            return Err(self.refuse(keyword, "must be an array of strings"));
        };
        if entries.is_empty() {
            return Err(self.refuse(keyword, "must name at least one property"));
        }
        let mut seen = HashSet::new();
        let mut names = Vec::with_capacity(entries.len());
        for entry in entries {
            let Bson::String(name) = entry else {
                return Err(self.refuse(keyword, "must be an array of strings"));
            };
            if !seen.insert(name.as_str()) {
                return Err(self.refuse(keyword, &format!("names {name:?} twice")));
            }
            names.push(name.clone());
        }
        Ok(names)
    }

    fn flag(&self, keyword: &str, value: &Bson) -> Result<bool> {
        match value {
            Bson::Boolean(b) => Ok(*b),
            _ => Err(self.refuse(keyword, "must be a boolean")),
        }
    }

    /// A bound: any number but a NaN, which orders against nothing.
    fn number(&self, keyword: &str, value: &Bson) -> Result<Bson> {
        if is_number(value) && !is_nan(value) {
            Ok(value.clone())
        } else {
            Err(self.refuse(keyword, "must be a number"))
        }
    }

    /// `multipleOf`: a finite number above zero.
    fn divisor(&self, keyword: &str, value: &Bson) -> Result<Bson> {
        let positive = match value {
            Bson::Int32(n) => *n > 0,
            Bson::Int64(n) => *n > 0,
            Bson::Double(d) => d.is_finite() && *d > 0.0,
            Bson::Decimal128(d) => {
                matches!(decimal::decode(d), Decimal::Finite { negative: false, .. })
            }
            _ => return Err(self.refuse(keyword, "must be a number greater than 0")),
        };
        if positive {
            Ok(value.clone())
        } else {
            Err(self.refuse(keyword, "must be a number greater than 0"))
        }
    }

    /// A length or a count: a whole number, zero or more.
    fn count(&self, keyword: &str, value: &Bson) -> Result<usize> {
        let whole = match value {
            Bson::Int32(n) => Some(i64::from(*n)),
            Bson::Int64(n) => Some(*n),
            Bson::Double(d) if d.is_finite() && d.fract() == 0.0 && d.abs() < 9.0e18 => {
                Some(*d as i64)
            }
            Bson::Decimal128(d) => decimal::whole_number(d),
            _ => None,
        };
        whole
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| self.refuse(keyword, "must be a non-negative whole number"))
    }

    fn regex(&self, keyword: &str, pattern: &str) -> Result<RegexPattern> {
        RegexPattern::new(pattern, "").map_err(|e| match e {
            Error::InvalidQuery(why) => {
                self.refuse(keyword, &format!("holds a pattern that {why}"))
            }
            other => other,
        })
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// The value under test: a document is borrowed as one, so the root of a
/// document is not copied into a [`Bson`] to be tested.
#[derive(Clone, Copy)]
enum V<'d> {
    Doc(&'d Document),
    Val(&'d Bson),
}

impl<'d> V<'d> {
    fn of(b: &'d Bson) -> V<'d> {
        match b {
            Bson::Document(d) => V::Doc(d),
            other => V::Val(other),
        }
    }

    fn type_name(self) -> &'static str {
        match self {
            V::Doc(_) => "object",
            V::Val(b) => type_name_of(b),
        }
    }
}

enum Seg {
    Key(String),
    Index(usize),
}

/// One test of a value, with the first failure kept when asked to explain.
struct Run {
    explain: bool,
    /// Where the value being tested stands, kept only when explaining.
    path: Vec<Seg>,
    /// Above zero inside `anyOf`, `oneOf` and `not`, where a failing
    /// sub-schema is not the reason the value fails.
    quiet: u32,
    reason: Option<String>,
}

impl Run {
    fn new(explain: bool) -> Self {
        Run { explain, path: Vec::new(), quiet: 0, reason: None }
    }

    /// Record why the value fails, if nothing has been recorded, and answer
    /// `false`.
    fn fail(&mut self, why: impl FnOnce() -> String) -> bool {
        if self.explain && self.quiet == 0 && self.reason.is_none() {
            let place = if self.path.is_empty() {
                "(document)".to_string()
            } else {
                let parts: Vec<String> = self
                    .path
                    .iter()
                    .map(|s| match s {
                        Seg::Key(k) => k.clone(),
                        Seg::Index(i) => i.to_string(),
                    })
                    .collect();
                parts.join(".")
            };
            self.reason = Some(format!("{place}: {}", why()));
        }
        false
    }

    /// The value under `key` against `node`.
    fn key(&mut self, key: &str, node: &Node, v: V<'_>, depth: usize) -> bool {
        if self.explain {
            self.path.push(Seg::Key(key.to_string()));
        }
        let ok = self.node(node, v, depth + 1);
        if self.explain {
            self.path.pop();
        }
        ok
    }

    /// The element at `at` against `node`.
    fn element(&mut self, at: usize, node: &Node, v: V<'_>, depth: usize) -> bool {
        if self.explain {
            self.path.push(Seg::Index(at));
        }
        let ok = self.node(node, v, depth + 1);
        if self.explain {
            self.path.pop();
        }
        ok
    }

    /// `node` against `v` with failures left unrecorded.
    fn quietly(&mut self, node: &Node, v: V<'_>, depth: usize) -> bool {
        self.quiet += 1;
        let ok = self.node(node, v, depth + 1);
        self.quiet -= 1;
        ok
    }

    fn node(&mut self, n: &Node, v: V<'_>, depth: usize) -> bool {
        // Parsing holds a schema to this depth, so this does not fire; it is
        // what keeps the recursion bounded if a schema were built another way.
        if depth > MAX_DEPTH {
            return self.fail(|| format!("schemas nest at most {MAX_DEPTH} levels deep"));
        }

        for (keyword, specs) in [("bsonType", &n.bson_type), ("type", &n.json_type)] {
            if let Some(specs) = specs
                && !specs.iter().any(|s| s.holds(v))
            {
                return self.fail(|| {
                    let wanted: Vec<&str> = specs.iter().map(|s| s.label).collect();
                    format!("{keyword} {} expected, found {}", wanted.join(" or "), v.type_name())
                });
            }
        }

        if let Some(values) = &n.enum_values {
            let found = match v {
                V::Val(b) => values.iter().any(|e| canonical_cmp(b, e) == Ordering::Equal),
                V::Doc(d) => {
                    let whole = Bson::Document(d.clone());
                    values.iter().any(|e| canonical_cmp(&whole, e) == Ordering::Equal)
                }
            };
            if !found {
                return self.fail(|| "enum does not list this value".into());
            }
        }

        for sub in &n.all_of {
            if !self.node(sub, v, depth + 1) {
                return false;
            }
        }
        if !n.any_of.is_empty() {
            let any = n.any_of.iter().any(|sub| self.quietly(sub, v, depth));
            if !any {
                return self.fail(|| "anyOf matches none of its schemas".into());
            }
        }
        if !n.one_of.is_empty() {
            let mut matched = 0;
            for sub in &n.one_of {
                if self.quietly(sub, v, depth) {
                    matched += 1;
                    if matched > 1 {
                        break;
                    }
                }
            }
            if matched == 0 {
                return self.fail(|| "oneOf matches none of its schemas".into());
            }
            if matched > 1 {
                return self.fail(|| "oneOf matches more than one of its schemas".into());
            }
        }
        if let Some(sub) = &n.not
            && self.quietly(sub, v, depth)
        {
            return self.fail(|| "not forbids the schema this value satisfies".into());
        }

        match v {
            V::Doc(doc) => self.object(n, doc, depth),
            V::Val(Bson::String(s)) => self.string(n, s),
            V::Val(Bson::Array(items)) => self.array(n, items, depth),
            V::Val(b) if is_number(b) => self.number(n, b),
            V::Val(_) => true,
        }
    }

    fn number(&mut self, n: &Node, b: &Bson) -> bool {
        if let Some(bound) = &n.minimum {
            let order = canonical_cmp(b, &bound.value);
            let ok =
                if bound.exclusive { order == Ordering::Greater } else { order != Ordering::Less };
            if !ok {
                return self.fail(|| {
                    let kind =
                        if bound.exclusive { "not above the exclusive" } else { "below the" };
                    format!("minimum: {b} is {kind} minimum {}", bound.value)
                });
            }
        }
        if let Some(bound) = &n.maximum {
            let order = canonical_cmp(b, &bound.value);
            let ok =
                if bound.exclusive { order == Ordering::Less } else { order != Ordering::Greater };
            if !ok {
                return self.fail(|| {
                    let kind =
                        if bound.exclusive { "not below the exclusive" } else { "above the" };
                    format!("maximum: {b} is {kind} maximum {}", bound.value)
                });
            }
        }
        if let Some(divisor) = &n.multiple_of
            && !is_multiple(b, divisor)
        {
            return self.fail(|| format!("multipleOf: {b} is not a multiple of {divisor}"));
        }
        true
    }

    fn string(&mut self, n: &Node, s: &str) -> bool {
        if n.min_length.is_some() || n.max_length.is_some() {
            // Characters, not bytes: a code point is one.
            let length = s.chars().count();
            if let Some(min) = n.min_length
                && length < min
            {
                return self.fail(|| format!("minLength: length {length} is below {min}"));
            }
            if let Some(max) = n.max_length
                && length > max
            {
                return self.fail(|| format!("maxLength: length {length} is above {max}"));
            }
        }
        if let Some(pattern) = &n.pattern
            && !pattern.is_match(s)
        {
            return self.fail(|| format!("pattern: does not match {:?}", pattern.pattern()));
        }
        true
    }

    fn array(&mut self, n: &Node, items: &[Bson], depth: usize) -> bool {
        if let Some(min) = n.min_items
            && items.len() < min
        {
            return self.fail(|| format!("minItems: {} items is below {min}", items.len()));
        }
        if let Some(max) = n.max_items
            && items.len() > max
        {
            return self.fail(|| format!("maxItems: {} items is above {max}", items.len()));
        }
        if n.unique_items && !all_distinct(items) {
            return self.fail(|| "uniqueItems: the array holds an item twice".into());
        }
        match &n.items {
            Items::Unset => {}
            Items::All(sub) => {
                for (at, item) in items.iter().enumerate() {
                    if !self.element(at, sub, V::of(item), depth) {
                        return false;
                    }
                }
            }
            Items::Each(subs) => {
                for (at, item) in items.iter().enumerate() {
                    let ok = match (subs.get(at), &n.additional_items) {
                        (Some(sub), _) => self.element(at, sub, V::of(item), depth),
                        (None, Additional::Allowed) => true,
                        (None, Additional::Denied) => {
                            return self.fail(|| {
                                format!(
                                    "additionalItems: item {at} is past the {} listed",
                                    subs.len()
                                )
                            });
                        }
                        (None, Additional::Schema(sub)) => {
                            self.element(at, sub, V::of(item), depth)
                        }
                    };
                    if !ok {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn object(&mut self, n: &Node, doc: &Document, depth: usize) -> bool {
        for name in &n.required {
            if !doc.contains_key(name) {
                return self.fail(|| format!("required: property {name:?} is missing"));
            }
        }
        if let Some(min) = n.min_properties
            && doc.len() < min
        {
            return self.fail(|| format!("minProperties: {} properties is below {min}", doc.len()));
        }
        if let Some(max) = n.max_properties
            && doc.len() > max
        {
            return self.fail(|| format!("maxProperties: {} properties is above {max}", doc.len()));
        }

        // A property the document lacks is not tested: only `required` asks
        // for it to be there.
        for (name, sub) in &n.properties {
            if let Some(value) = doc.get(name)
                && !self.key(name, sub, V::of(value), depth)
            {
                return false;
            }
        }

        if !n.pattern_properties.is_empty()
            || !matches!(n.additional_properties, Additional::Allowed)
        {
            for (key, value) in doc {
                let mut named = n.property_at.contains_key(key.as_str());
                for (pattern, sub) in &n.pattern_properties {
                    if pattern.is_match(key) {
                        named = true;
                        if !self.key(key, sub, V::of(value), depth) {
                            return false;
                        }
                    }
                }
                if named {
                    continue;
                }
                match &n.additional_properties {
                    Additional::Allowed => {}
                    Additional::Denied => {
                        return self.fail(|| {
                            format!("additionalProperties: property {key:?} is not allowed")
                        });
                    }
                    Additional::Schema(sub) => {
                        if !self.key(key, sub, V::of(value), depth) {
                            return false;
                        }
                    }
                }
            }
        }

        for (name, dependency) in &n.dependencies {
            if !doc.contains_key(name) {
                continue;
            }
            match dependency {
                Dependency::Properties(needed) => {
                    if let Some(missing) = needed.iter().find(|p| !doc.contains_key(p.as_str())) {
                        return self
                            .fail(|| format!("dependencies: {name:?} needs property {missing:?}"));
                    }
                }
                Dependency::Schema(sub) => {
                    if !self.node(sub, V::Doc(doc), depth + 1) {
                        return false;
                    }
                }
            }
        }
        true
    }
}

/// Whether no two items are equal in the canonical order, so `1` and `1.0`
/// are one item. Sorted, so n log n comparisons.
fn all_distinct(items: &[Bson]) -> bool {
    match items {
        [] | [_] => true,
        [a, b] => canonical_cmp(a, b) != Ordering::Equal,
        _ => {
            let mut sorted: Vec<&Bson> = items.iter().collect();
            sorted.sort_unstable_by(|a, b| canonical_cmp(a, b));
            sorted.windows(2).all(|w| canonical_cmp(w[0], w[1]) != Ordering::Equal)
        }
    }
}

/// Whether `value` is a whole multiple of `divisor`, a number above zero.
///
/// Both numbers are read as decimals and the remainder is taken exactly, which is
/// the reference's rule (it converts each side to a `Decimal128` and tests the
/// remainder for zero). An `int`, a `long` and a `decimal` are read as they are. A
/// `double` is first rounded to 15 significant decimal digits, the precision a
/// double is reliably printed at, so `0.3` is a multiple of `0.1`, `19.99` of `0.01`
/// and `4.35` of `0.05`, as they read on the page, though none of them is stored
/// exactly in binary. A value that is not finite is a multiple of nothing; `0` is
/// a multiple of every divisor.
///
/// Nothing here grows with the size of the numbers: a coefficient is at most 34
/// digits and the exponents are only compared, so `1e300` against `0.5` is decided
/// in a few operations (it is a multiple).
fn is_multiple(value: &Bson, divisor: &Bson) -> bool {
    let (Some((cv, ev)), Some((cd, ed))) = (decimal_parts(value), decimal_parts(divisor)) else {
        return false;
    };
    if cd == 0 {
        return false;
    }
    if cv == 0 {
        return true;
    }
    decimal_multiple(cv, ev, cd, ed)
}

/// A double as `coefficient * 10^exponent`, rounded to 15 significant digits, the
/// way the reference converts one to a `Decimal128`; `None` for a `NaN` or an
/// infinity. A zero is `(0, 0)`.
fn double_parts(d: f64) -> Option<(u128, i32)> {
    if !d.is_finite() {
        return None;
    }
    if d == 0.0 {
        return Some((0, 0));
    }
    // `{:.14e}` is one digit, a point, fourteen digits, `e`, the decimal exponent:
    // 15 significant digits, rounded from the exact binary value.
    let text = format!("{:.14e}", d.abs());
    let (mantissa, exponent) = text.split_once('e')?;
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let coefficient = digits.parse::<u128>().ok()?;
    let exponent = exponent.parse::<i32>().ok()?;
    Some((coefficient, exponent - 14))
}

/// The magnitude of a finite number as `coefficient * 10^exponent`, a zero as
/// `(0, 0)`; `None` for a NaN, an infinity or a value that is not a number. A double
/// is rounded first ([`double_parts`]).
fn decimal_parts(b: &Bson) -> Option<(u128, i32)> {
    match b {
        Bson::Int32(n) => Some((u128::from(n.unsigned_abs()), 0)),
        Bson::Int64(n) => Some((u128::from(n.unsigned_abs()), 0)),
        Bson::Double(d) => double_parts(*d),
        Bson::Decimal128(d) => match decimal::decode(d) {
            Decimal::Zero => Some((0, 0)),
            Decimal::Finite { coefficient, exponent, .. } => {
                Some((coefficient, i32::from(exponent)))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Whether `cv * 10^ev` is a whole multiple of `cd * 10^ed`, both above zero,
/// decided in `u128` with no rounding.
fn decimal_multiple(cv: u128, ev: i32, cd: u128, ed: i32) -> bool {
    let shift = ev - ed;
    if shift >= 0 {
        // cd must divide cv * 10^shift: once cv's share of cd is cancelled,
        // what is left has to be made of the twos and fives of a power of ten.
        let mut rest = cd / gcd(cv, cd);
        let (mut twos, mut fives) = (0, 0);
        while rest.is_multiple_of(2) {
            rest /= 2;
            twos += 1;
        }
        while rest.is_multiple_of(5) {
            rest /= 5;
            fives += 1;
        }
        rest == 1 && twos <= shift && fives <= shift
    } else {
        // cv must be cd * 10^-shift times a whole number. cv is below 10^34,
        // so it holds no power of ten past that.
        let Some(power) = u32::try_from(-shift).ok().and_then(|k| 10u128.checked_pow(k)) else {
            return false;
        };
        cv.is_multiple_of(cd) && (cv / cd).is_multiple_of(power)
    }
}

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
