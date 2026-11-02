//! `$sum`, `$avg`, `$min` and `$max` as expressions: the reference's answers for 392 calls
//! (`tests/data/q8d_fold.tsv`, recorded from the reference (6.0.5) by `q8-probes/q8d-agg.js`), which
//! cover every width and type of operand, the single-expression form and the list form, the
//! order across types, NaN, the infinities, null and missing values and `Decimal128` sums.
//! Types are compared exactly (`Int32` beside `Int64` beside a double), a double bit for bit.
//!
//! A line of the fixture is `operator`, a label, the operands (`|` apart; a leading `=` means
//! the operand is the operator's whole argument, not an element of a list) and the answer. A
//! value is spelled `null`, `iN`, `lN`, `dX`, `DX`, `T`, `F`, `tMS` (a date), `sTEXT` (a
//! string; `$` names a field), `A[v,v]`, `O{k=v,k=v}`, `Q<v>` (the literal of `v`), `oidHEX`,
//! `binBASE64`, `minkey`, `maxkey`, `rxPATTERN` and `tsT:I`.

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, Timestamp, doc};
use kimmy_core::decimal::parse;

use crate::expr::Expr;

struct Cursor<'a> {
    text: &'a str,
}

impl<'a> Cursor<'a> {
    fn eat(&mut self, prefix: &str) -> bool {
        match self.text.strip_prefix(prefix) {
            Some(rest) => {
                self.text = rest;
                true
            }
            None => false,
        }
    }

    /// The text up to the next delimiter of the grammar.
    fn word(&mut self) -> &'a str {
        let end = self.text.find([',', ']', '}']).unwrap_or(self.text.len());
        let (word, rest) = self.text.split_at(end);
        self.text = rest;
        word
    }

    fn value(&mut self) -> Bson {
        if self.eat("null") {
            return Bson::Null;
        }
        if self.eat("minkey") {
            return Bson::MinKey;
        }
        if self.eat("maxkey") {
            return Bson::MaxKey;
        }
        if self.eat("Q") {
            return Bson::Document(doc! { "$literal": self.value() });
        }
        if self.eat("A[") {
            let mut items = Vec::new();
            while !self.eat("]") {
                self.eat(",");
                items.push(self.value());
            }
            return Bson::Array(items);
        }
        if self.eat("O{") {
            let mut fields = Document::new();
            while !self.eat("}") {
                self.eat(",");
                let key = self.text.split('=').next().unwrap().to_string();
                self.text = &self.text[key.len() + 1..];
                let value = self.value();
                fields.insert(key, value);
            }
            return Bson::Document(fields);
        }
        if self.eat("oid") {
            let hex = self.word();
            return Bson::ObjectId(bson::oid::ObjectId::parse_str(hex).unwrap());
        }
        if self.eat("bin") {
            let word = self.word();
            let bytes = match word {
                "AQ==" => vec![1],
                "Ag==" => vec![2],
                "AQID" => vec![1, 2, 3],
                other => panic!("binary {other}"),
            };
            return Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes });
        }
        if self.eat("rx") {
            let pattern = self.word().to_string();
            return Bson::RegularExpression(bson::Regex {
                pattern: pattern.try_into().expect("valid pattern"),
                options: String::new().try_into().expect("valid options"),
            });
        }
        if self.eat("ts") {
            let (t, i) = self.word().split_once(':').unwrap();
            return Bson::Timestamp(Timestamp {
                time: t.parse().unwrap(),
                increment: i.parse().unwrap(),
            });
        }
        let word = self.word();
        match &word[..1] {
            "i" => Bson::Int32(word[1..].parse().unwrap()),
            "l" => Bson::Int64(word[1..].parse().unwrap()),
            "d" => Bson::Double(word[1..].parse().unwrap()),
            "D" => Bson::Decimal128(parse(&word[1..]).unwrap()),
            "T" => Bson::Boolean(true),
            "F" => Bson::Boolean(false),
            "t" => Bson::DateTime(bson::DateTime::from_millis(word[1..].parse().unwrap())),
            "s" => Bson::String(word[1..].to_string()),
            _ => panic!("value {word}"),
        }
    }
}

fn value(code: &str) -> Bson {
    Cursor { text: code }.value()
}

/// Whether two values are the same, down to the type, a double bit for bit (a NaN is a NaN).
fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|((k, p), (l, q))| k == l && same(p, q))
        }
        _ => std::mem::discriminant(a) == std::mem::discriminant(b) && a == b,
    }
}

/// The document the field paths of the fixture read.
fn document() -> Document {
    doc! {
        "nul": Bson::Null,
        "i": 7,
        "l": 7i64,
        "d": 0.5,
        "dec": Bson::Decimal128(parse("0.5").unwrap()),
        "s": "str",
        "arr": [1, 2, 3],
        "bin": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] },
        "ts": Timestamp { time: 1_700_000_000, increment: 5 },
    }
}

fn eval(name: &str, argument: Bson) -> Bson {
    let call = Bson::Document(doc! { name: argument });
    Expr::parse(&call)
        .unwrap_or_else(|e| panic!("{call:?}: {e}"))
        .eval(&document())
        .unwrap_or_else(|e| panic!("{call:?}: {e}"))
}

fn run(name: &str, operands: &str) -> Bson {
    let argument = match operands.strip_prefix('=') {
        Some(whole) => value(whole),
        None if operands.is_empty() => Bson::Array(Vec::new()),
        None => Bson::Array(operands.split('|').map(value).collect()),
    };
    eval(name, argument)
}

/// The calls whose answer here is not the reference's, and what it is here: the arithmetic is the
/// `$group` accumulators', which `docs/deviations.md` records. A double sum adds one value at a
/// time (the reference's double-double sum keeps the bits that an `Int64` beyond 2^53 loses), and
/// a decimal sum is exact and rounded once (the reference rounds each partial sum to 34 digits,
/// so it loses a `1` between two `1E+6000`s and overflows where the exact sum does not).
const DEVIATIONS: &[(&str, &str, &str)] = &[
    ("$sum", "2^53", "d9007199254740992.0"),
    ("$avg", "2^53", "d4503599627370496.0"),
    ("$sum", "dec exact", "D1"),
    ("$avg", "dec exact", "D0.3333333333333333333333333333333333"),
    ("$avg", "dec big", "D9.999999999999999999999999999999999E+6144"),
];

#[test]
fn every_recorded_answer_is_the_references() {
    let table = include_str!("../tests/data/q8d_fold.tsv");
    let mut wrong = Vec::new();
    let mut checked = 0;
    let mut deviating = 0;
    for line in table.lines() {
        let mut cols = line.split('\t');
        let (name, label, operands, want) = (
            cols.next().unwrap(),
            cols.next().unwrap(),
            cols.next().unwrap(),
            cols.next().unwrap(),
        );
        checked += 1;
        let got = run(name, operands);
        if let Some((_, _, here)) = DEVIATIONS.iter().find(|(n, l, _)| *n == name && *l == label) {
            deviating += 1;
            assert!(same(&got, &value(here)), "{name} {label}: {got:?}, expected {here}");
            assert!(
                !same(&got, &value(want)),
                "{name} {label} now agrees: drop it from DEVIATIONS"
            );
        } else if !same(&got, &value(want)) {
            wrong.push(format!("{name} {label}: want {want}, got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} of {checked}:\n{}", wrong.len(), wrong.join("\n"));
    assert!(checked >= 380, "{checked}");
    assert_eq!(deviating, DEVIATIONS.len());
}

#[test]
fn a_list_of_nulls_is_null_or_zero() {
    assert_eq!(run("$sum", "null|null"), Bson::Int32(0));
    assert!(matches!(run("$avg", "null|null"), Bson::Null));
    assert!(matches!(run("$min", "null|null"), Bson::Null));
    assert!(matches!(run("$max", "null|null"), Bson::Null));
}

#[test]
fn the_operators_nest_and_work_beside_others() {
    // The reference: `$sum` of an `$avg` and 1 is the double 2.5; `$max` decides a `$cond`.
    assert_eq!(
        eval("$sum", Bson::Array(vec![Bson::Document(doc! { "$avg": [1, 2] }), Bson::Int32(1)])),
        Bson::Double(2.5)
    );
    let cond = Bson::Document(doc! { "$cond": [{ "$gt": [{ "$max": [1, 2] }, 1] }, "y", "n"] });
    assert_eq!(Expr::parse(&cond).unwrap().eval(&document()).unwrap(), Bson::String("y".into()));
    // As a field of `$addFields`/`$project`: the reference's `{s: 8, m: 7}` for `$i` 7.
    let spec = Bson::Document(doc! { "s": { "$sum": ["$i", 1] }, "m": { "$max": ["$i", 1] } });
    let out = Expr::parse(&spec).unwrap().eval(&document()).unwrap();
    assert_eq!(out, Bson::Document(doc! { "s": 8, "m": 7 }));
}

#[test]
fn the_operators_read_the_documents_fields_and_variables() {
    // A field that holds an array is the array's elements; `$map` builds one.
    let map = doc! { "$map": { "input": "$arr", "as": "x", "in": { "$multiply": ["$$x", 2] } } };
    assert_eq!(eval("$sum", Bson::Document(map.clone())), Bson::Int32(12));
    assert_eq!(eval("$avg", Bson::Document(map.clone())), Bson::Double(4.0));
    assert_eq!(eval("$min", Bson::Document(map.clone())), Bson::Int32(2));
    assert_eq!(eval("$max", Bson::Document(map)), Bson::Int32(6));
    let reduce = doc! { "$reduce": {
    "input": "$arr", "initialValue": 0,
    "in": { "$add": ["$$value", { "$max": ["$$this", 2] }] } } };
    assert_eq!(
        Expr::parse(&Bson::Document(reduce)).unwrap().eval(&document()).unwrap(),
        Bson::Int32(7)
    );
}
