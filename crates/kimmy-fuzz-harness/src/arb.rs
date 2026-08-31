//! Structured inputs drawn from fuzzer bytes.
//!
//! A fuzzer that feeds raw bytes into a JSON parser spends most of its budget
//! learning JSON syntax. For the targets whose input is a *value* rather than
//! text — the key encoder, the token claims — it is far more productive to let
//! the bytes choose among shapes we already know are interesting, and let
//! coverage feedback find the combinations. That is what [`Unstructured`] is
//! for: every choice below consumes a little of the input, and mutating the
//! input mutates the choices.
//!
//! Two properties of `Unstructured` matter for how this is written. When the
//! bytes run out it keeps answering — a range pick returns its lower bound, a
//! string is empty — so generation always terminates and never fails; and the
//! *depth* of what it builds is bounded here explicitly rather than by the
//! input, because the key encoder and the comparator both recurse.

use arbitrary::Unstructured;
use bson::{Bson, Document};
use serde_json::{Map, Value, json};

/// How deep a generated document or array may nest.
const MAX_DEPTH: u8 = 4;
/// How many elements a generated document or array may hold.
const MAX_WIDTH: usize = 4;

/// Strings chosen so that pairs collide, share prefixes, and cross the NUL
/// framing the encoder escapes: `"ab"` must sort before `"ab\0c"`.
const STRINGS: [&str; 8] = ["", "a", "b", "aa", "ab", "ab\u{0}", "ab\u{0}c", "\u{0}"];
/// Keys with the same intent, plus the one the update language protects.
const KEYS: [&str; 5] = ["a", "b", "aa", "_id", ""];
/// The doubles where a sign/magnitude decomposition is most likely to be
/// wrong: both zeros, the infinities, `NaN`, the subnormal floor, the smallest
/// normal, the largest finite, and the two integer-precision edges.
const DOUBLES: [f64; 11] = [
    0.0,
    -0.0,
    f64::INFINITY,
    f64::NEG_INFINITY,
    f64::NAN,
    5e-324,
    f64::MIN_POSITIVE,
    f64::MAX,
    9_007_199_254_740_992.0,
    9_007_199_254_740_993.0,
    -1.0,
];
/// Integers at the type-width edges, where `Int32`/`Int64`/`Double` must
/// agree on order across the boundary each one cannot represent.
const INTS: [i64; 9] =
    [0, 1, -1, i32::MAX as i64, i32::MIN as i64, i32::MAX as i64 + 1, i64::MAX, i64::MIN, 1 << 53];

/// One BSON value, of any variant the crate can construct.
pub fn bson(u: &mut Unstructured<'_>) -> Bson {
    bson_at(u, 0)
}

fn bson_at(u: &mut Unstructured<'_>, depth: u8) -> Bson {
    // The composite variants sit at the top of the range so that an exhausted
    // input (which answers the lower bound) yields a leaf and recursion ends
    // by itself as well as by `MAX_DEPTH`.
    let top = if depth >= MAX_DEPTH { 19 } else { 22 };
    match u.int_in_range(0..=top).unwrap_or(0) {
        0 => Bson::Null,
        1 => Bson::MinKey,
        2 => Bson::MaxKey,
        3 => Bson::Undefined,
        4 => Bson::Boolean(u.arbitrary().unwrap_or(false)),
        5 => Bson::Int32(*u.choose(&INTS).unwrap_or(&0) as i32),
        6 => Bson::Int32(u.arbitrary().unwrap_or(0)),
        7 => Bson::Int64(*u.choose(&INTS).unwrap_or(&0)),
        8 => Bson::Int64(u.arbitrary().unwrap_or(0)),
        9 => Bson::Double(*u.choose(&DOUBLES).unwrap_or(&0.0)),
        10 => Bson::Double(u.arbitrary().unwrap_or(0.0)),
        11 => Bson::String(string(u)),
        12 => Bson::Symbol(string(u)),
        13 => Bson::DateTime(bson::DateTime::from_millis(u.arbitrary().unwrap_or(0))),
        14 => Bson::ObjectId(bson::oid::ObjectId::from_bytes(u.arbitrary().unwrap_or([0; 12]))),
        15 => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::from(u.arbitrary::<u8>().unwrap_or(0)),
            bytes: u.arbitrary().unwrap_or_default(),
        }),
        16 => Bson::Timestamp(bson::Timestamp {
            time: u.arbitrary().unwrap_or(0),
            increment: u.arbitrary().unwrap_or(0),
        }),
        17 => Bson::JavaScriptCode(string(u)),
        18 => regex(u).unwrap_or(Bson::Null),
        // Deliberately rare: it is the one variant the encoder refuses, and
        // a generator that produced it often would spend its budget on the
        // refusal path.
        19 if u.arbitrary::<u8>().unwrap_or(0) == 0 => {
            Bson::Decimal128(bson::Decimal128::from_bytes(u.arbitrary().unwrap_or([0; 16])))
        }
        19 => Bson::Int32(0),
        20 => Bson::Array(array(u, depth + 1)),
        21 => Bson::Document(document(u, depth + 1)),
        _ => Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
            code: string(u),
            scope: document(u, depth + 1),
        }),
    }
}

fn string(u: &mut Unstructured<'_>) -> String {
    if u.arbitrary::<bool>().unwrap_or(true) {
        (*u.choose(&STRINGS).unwrap_or(&"")).to_string()
    } else {
        u.arbitrary::<String>().unwrap_or_default()
    }
}

fn regex(u: &mut Unstructured<'_>) -> Option<Bson> {
    let pattern = bson::raw::CString::try_from(string(u)).ok()?;
    let options =
        bson::raw::CString::try_from((*u.choose(&["", "i", "im"]).ok()?).to_string()).ok()?;
    Some(Bson::RegularExpression(bson::Regex { pattern, options }))
}

fn array(u: &mut Unstructured<'_>, depth: u8) -> Vec<Bson> {
    let len = u.int_in_range(0..=MAX_WIDTH).unwrap_or(0);
    (0..len).map(|_| bson_at(u, depth)).collect()
}

fn document(u: &mut Unstructured<'_>, depth: u8) -> Document {
    let len = u.int_in_range(0..=MAX_WIDTH).unwrap_or(0);
    let mut doc = Document::new();
    for _ in 0..len {
        let key = if u.arbitrary::<bool>().unwrap_or(true) {
            (*u.choose(&KEYS).unwrap_or(&"a")).to_string()
        } else {
            u.arbitrary::<String>().unwrap_or_default()
        };
        doc.insert(key, bson_at(u, depth));
    }
    doc
}

/// Whether any document inside a value has a key starting with `$`.
///
/// Such a document is indistinguishable, once printed as JSON, from an
/// Extended JSON wrapper, so the boundary may legitimately refuse or
/// reinterpret it; the round-trip claims are made only for values without one.
pub fn has_dollar_key(value: &Bson) -> bool {
    match value {
        Bson::Array(items) => items.iter().any(has_dollar_key),
        Bson::Document(doc) => {
            doc.iter().any(|(key, value)| key.starts_with('$') || has_dollar_key(value))
        }
        Bson::JavaScriptCodeWithScope(cws) => has_dollar_key(&Bson::Document(cws.scope.clone())),
        _ => false,
    }
}

/// Whether a value contains a `Decimal128` anywhere, which is the one thing
/// the key encoder is allowed to refuse.
pub fn holds_decimal(value: &Bson) -> bool {
    match value {
        Bson::Decimal128(_) => true,
        Bson::Array(items) => items.iter().any(holds_decimal),
        Bson::Document(doc) => doc.values().any(holds_decimal),
        Bson::JavaScriptCodeWithScope(cws) => cws.scope.values().any(holds_decimal),
        _ => false,
    }
}

/// A JWT claims document for the verifiers to sign and check.
///
/// Weighted toward well-formed: the registered claims are usually present and
/// usually right, so the fuzzer gets past issuer, audience and expiry often
/// enough to spend its time in what comes after — the grants and roles
/// parsers, the role-claim lookup, and the principal those build. Each claim
/// still has a wrong or missing form the bytes can select.
///
/// `audience` is `None` for the local verifier, which checks no audience and
/// refuses a token that carries one; the claim is then usually absent and
/// occasionally garbage, never the federated value.
pub fn claims(u: &mut Unstructured<'_>, issuer: &str, audience: Option<&str>) -> Value {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut claims = Map::new();

    let mut put = |key: &str, value: Option<Value>| {
        if let Some(value) = value {
            claims.insert(key.to_string(), value);
        }
    };

    put(
        "sub",
        match u.int_in_range(0..=7).unwrap_or(0) {
            0 => None,
            1 => Some(json!("")),
            2 => Some(json!(42)),
            _ => Some(json!(string(u))),
        },
    );
    put(
        "iss",
        match u.int_in_range(0..=7).unwrap_or(0) {
            0 => None,
            1 => Some(json!(string(u))),
            _ => Some(json!(issuer)),
        },
    );
    put(
        "aud",
        match (audience, u.int_in_range(0..=7).unwrap_or(0)) {
            (_, 0) => None,
            (_, 1) => Some(json!(string(u))),
            (Some(audience), 2) => Some(json!([audience, string(u)])),
            (Some(audience), _) => Some(json!(audience)),
            (None, _) => None,
        },
    );
    put(
        "exp",
        match u.int_in_range(0..=7).unwrap_or(0) {
            0 => None,
            1 => Some(json!(u.arbitrary::<i64>().unwrap_or(0))),
            2 => Some(json!(now.saturating_sub(120))),
            3 => Some(json!("soon")),
            _ => Some(json!(now + 600)),
        },
    );
    put(
        "iat",
        match u.int_in_range(0..=3).unwrap_or(0) {
            0 => None,
            1 => Some(json!(u.arbitrary::<i64>().unwrap_or(0))),
            _ => Some(json!(now)),
        },
    );
    put(
        "nbf",
        match u.int_in_range(0..=3).unwrap_or(0) {
            0 | 1 => None,
            2 => Some(json!(u.arbitrary::<i64>().unwrap_or(0))),
            _ => Some(json!(now.saturating_sub(5))),
        },
    );
    put(
        "tv",
        match u.int_in_range(0..=3).unwrap_or(0) {
            0 => None,
            1 => Some(json!(u.arbitrary::<u64>().unwrap_or(0))),
            2 => Some(json!(-1)),
            _ => Some(json!(0)),
        },
    );
    put(
        "roles",
        match u.int_in_range(0..=3).unwrap_or(0) {
            0 => None,
            1 => Some(json!(string(u))),
            _ => Some(Value::Array(
                (0..u.int_in_range(0..=3).unwrap_or(0)).map(|_| role(u)).collect(),
            )),
        },
    );
    put(
        "grants",
        match u.int_in_range(0..=3).unwrap_or(0) {
            0 => None,
            1 => Some(json!({})),
            _ => Some(Value::Array(
                (0..u.int_in_range(0..=3).unwrap_or(0)).map(|_| grant(u)).collect(),
            )),
        },
    );
    Value::Object(claims)
}

fn role(u: &mut Unstructured<'_>) -> Value {
    match u.int_in_range(0..=4).unwrap_or(0) {
        0 => json!("kimmydb-analyst"),
        1 => json!("analyst"),
        2 => json!(7),
        _ => json!(string(u)),
    }
}

fn grant(u: &mut Unstructured<'_>) -> Value {
    const ACTIONS: [&str; 8] =
        ["read", "write", "watch", "search", "webhook", "ddl", "admin", "Read"];
    let actions: Vec<Value> = (0..u.int_in_range(0..=3).unwrap_or(0))
        .map(|_| json!(u.choose(&ACTIONS).copied().unwrap_or("read")))
        .collect();
    let mut grant = Map::new();
    if u.arbitrary::<bool>().unwrap_or(true) {
        grant.insert(
            "db".into(),
            json!(u.choose(&["sales", "*", "__kimmy", "sal*"]).copied().unwrap_or("sales")),
        );
    }
    if u.arbitrary::<bool>().unwrap_or(true) {
        grant.insert(
            "collection".into(),
            json!(u.choose(&["orders", "orders*", "*", "", "**"]).copied().unwrap_or("*")),
        );
    }
    if u.arbitrary::<bool>().unwrap_or(true) {
        grant.insert("actions".into(), Value::Array(actions));
    }
    Value::Object(grant)
}
