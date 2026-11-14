//! The bridge between BSON and the merge state's [`Value`], both directions (Q7 Step 2).
//!
//! One function pair for every caller (the query side's effects and the storage side's insert,
//! replace and render), so that a BSON value means one thing to the merge state.
//!
//! # What maps to what
//!
//! Every BSON type has a [`Value`] variant except six, which the merge state cannot hold and
//! which are refused ([`InvalidValue::UnsupportedType`], a 400 `BadValue` on a `field`
//! collection, listed in `docs/deviations.md`): JavaScript code (with or without scope), symbol,
//! DB pointer, min key, max key and undefined. A BSON document becomes [`Value::Document`] and an
//! array [`Value::Array`], recursively; where a document may stand (only inside an array as a
//! register) is the writer's rule ([`Value::check_input`]), not this module's.
//!
//! Going back, [`Value::Map`] and [`Value::ImplicitMap`] read as an empty document (as the
//! writer's own read does) and [`Value::Unset`] has no BSON value and is an error: a caller never
//! holds one in a document it reads.

use bson::{Bson, Document};

use super::ord::OrderedValue;
use super::types::{InvalidValue, Path, Value};

/// The BSON type name of a value the merge state cannot hold, or `None` when it can.
fn unsupported(b: &Bson) -> Option<&'static str> {
    match b {
        Bson::JavaScriptCode(_) => Some("javascript"),
        Bson::JavaScriptCodeWithScope(_) => Some("javascriptWithScope"),
        Bson::Symbol(_) => Some("symbol"),
        Bson::DbPointer(_) => Some("dbPointer"),
        Bson::MinKey => Some("minKey"),
        Bson::MaxKey => Some("maxKey"),
        Bson::Undefined => Some("undefined"),
        _ => None,
    }
}

/// The merge-state value of a BSON value. A BSON document is a [`Value::Document`].
pub fn bson_to_value(b: &Bson) -> Result<Value, InvalidValue> {
    if let Some(name) = unsupported(b) {
        return Err(InvalidValue::UnsupportedType(name));
    }
    Ok(match b {
        Bson::Null => Value::Null,
        Bson::Int32(n) => Value::Int32(*n),
        Bson::Int64(n) => Value::Int64(*n),
        Bson::Double(d) => Value::Double(*d),
        Bson::Decimal128(d) => Value::Decimal128(d.bytes()),
        Bson::String(s) => Value::String(s.clone()),
        Bson::Boolean(v) => Value::Bool(*v),
        Bson::DateTime(d) => Value::Date(d.timestamp_millis()),
        Bson::ObjectId(o) => Value::ObjectId(o.bytes()),
        Bson::Binary(bin) => {
            Value::Binary { subtype: u8::from(bin.subtype), bytes: bin.bytes.clone() }
        }
        Bson::Timestamp(t) => Value::Timestamp { time: t.time, increment: t.increment },
        Bson::RegularExpression(r) => Value::Regex {
            pattern: String::from(r.pattern.clone()),
            options: String::from(r.options.clone()),
        },
        Bson::Array(items) => {
            Value::Array(items.iter().map(bson_to_value).collect::<Result<_, _>>()?)
        }
        Bson::Document(d) => document_to_value(d)?,
        // the unsupported types returned above
        _ => return Err(InvalidValue::UnsupportedType("unknown")),
    })
}

/// A BSON document as a [`Value::Document`], fields in order.
pub fn document_to_value(d: &Document) -> Result<Value, InvalidValue> {
    let mut fields = Vec::with_capacity(d.len());
    for (name, v) in d {
        fields.push((name.clone(), bson_to_value(v)?));
    }
    Ok(Value::Document(fields))
}

/// The top-level fields of a BSON document as `(Path of one segment, value)`, in order, the shape
/// of `Writer::insert` and `write_replace`: a field name is ONE segment, never split on `.`. A
/// field name the merge state cannot hold as a segment (a NUL byte) is
/// [`InvalidValue::BadFieldName`].
pub fn document_to_fields(d: &Document) -> Result<Vec<(Path, Value)>, InvalidValue> {
    let mut out = Vec::with_capacity(d.len());
    for (name, v) in d {
        let path = Path::flat(name).map_err(|_| InvalidValue::BadFieldName)?;
        out.push((path, bson_to_value(v)?));
    }
    Ok(out)
}

/// The BSON value of a merge-state value: [`Value::Document`] is a document, the map markers an
/// empty one. `None` for [`Value::Unset`] (nothing to read).
pub fn value_to_bson(v: &Value) -> Option<Bson> {
    Some(match v {
        Value::Unset => return None,
        Value::Null => Bson::Null,
        Value::Int32(n) => Bson::Int32(*n),
        Value::Int64(n) => Bson::Int64(*n),
        Value::Double(d) => Bson::Double(*d),
        Value::Decimal128(b) => Bson::Decimal128(bson::Decimal128::from_bytes(*b)),
        Value::String(s) => Bson::String(s.clone()),
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Date(ms) => Bson::DateTime(bson::DateTime::from_millis(*ms)),
        Value::ObjectId(b) => Bson::ObjectId(bson::oid::ObjectId::from_bytes(*b)),
        Value::Binary { subtype, bytes } => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::from(*subtype),
            bytes: bytes.clone(),
        }),
        Value::Timestamp { time, increment } => {
            Bson::Timestamp(bson::Timestamp { time: *time, increment: *increment })
        }
        Value::Regex { pattern, options } => Bson::RegularExpression(bson::Regex {
            pattern: bson::raw::CString::try_from(pattern.as_str()).ok()?,
            options: bson::raw::CString::try_from(options.as_str()).ok()?,
        }),
        Value::Array(items) => Bson::Array(items.iter().filter_map(value_to_bson).collect()),
        Value::Document(fields) => Bson::Document(fields_to_document(fields)),
        Value::Map | Value::ImplicitMap => Bson::Document(Document::new()),
    })
}

fn fields_to_document(fields: &[(String, Value)]) -> Document {
    let mut d = Document::new();
    for (name, v) in fields {
        if let Some(b) = value_to_bson(v) {
            d.insert(name.clone(), b);
        }
    }
    d
}

/// A [`Value::Document`] (or a map marker) as a BSON document; `None` for any other value.
pub fn value_to_document(v: &Value) -> Option<Document> {
    match value_to_bson(v)? {
        Bson::Document(d) => Some(d),
        _ => None,
    }
}

/// The document of a writer's ordered read ([`MergeState::ordered_document`](super::MergeState)):
/// a level of the tree is a BSON document, fields in the order the read gives them, a leaf its
/// BSON value. A field name is kept whole (`a.b` stays one field).
pub fn ordered_to_document(level: &[(String, OrderedValue)]) -> Document {
    let mut d = Document::new();
    for (name, v) in level {
        match v {
            OrderedValue::Leaf(leaf) => {
                if let Some(b) = value_to_bson(leaf) {
                    d.insert(name.clone(), b);
                }
            }
            OrderedValue::Doc(inner) => {
                d.insert(name.clone(), Bson::Document(ordered_to_document(inner)));
            }
        }
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, oid::ObjectId};

    fn all_supported() -> Document {
        doc! {
            "null": Bson::Null, "i32": 7i32, "i64": 1i64 << 40, "dbl": -0.0f64, "nan": f64::NAN,
            "dec": Bson::Decimal128(bson::Decimal128::from_bytes([3; 16])),
            "s": "x\u{e9}", "b": true,
            "date": Bson::DateTime(bson::DateTime::from_millis(-5)),
            "oid": ObjectId::from_bytes([9; 12]),
            "bin": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::UserDefined(0x90), bytes: vec![1, 2] }),
            "ts": Bson::Timestamp(bson::Timestamp { time: 5, increment: 6 }),
            "re": Bson::RegularExpression(bson::Regex {
                pattern: "a.b".try_into().unwrap(), options: "i".try_into().unwrap() }),
            "arr": [1, [2, { "k": [3] }], { "d.ot": 1 }],
            "sub": { "x": 1, "a.b": { "y": Bson::Null } },
        }
    }

    #[test]
    fn every_supported_type_round_trips_with_bits() {
        let d = all_supported();
        let v = document_to_value(&d).unwrap();
        let back = value_to_document(&v).unwrap();
        // compare through the encoding: NaN and -0.0 bit patterns included
        let (mut a, mut b) = (Vec::new(), Vec::new());
        d.to_writer(&mut a).unwrap();
        back.to_writer(&mut b).unwrap();
        assert_eq!(a, b);
        assert_eq!(back.keys().collect::<Vec<_>>(), d.keys().collect::<Vec<_>>());
    }

    #[test]
    fn the_types_the_state_cannot_hold_are_refused_at_any_depth() {
        let js = Bson::JavaScriptCode("f()".into());
        let cases: Vec<(Bson, &str)> = vec![
            (js.clone(), "javascript"),
            (
                Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                    code: "f".into(),
                    scope: doc! {},
                }),
                "javascriptWithScope",
            ),
            (Bson::Symbol("s".into()), "symbol"),
            (Bson::MinKey, "minKey"),
            (Bson::MaxKey, "maxKey"),
            (Bson::Undefined, "undefined"),
        ];
        for (b, name) in cases {
            let want = Err(InvalidValue::UnsupportedType(name));
            assert_eq!(bson_to_value(&b), want);
            assert_eq!(bson_to_value(&Bson::Array(vec![Bson::Int32(1), b.clone()])), want);
            assert_eq!(document_to_value(&doc! { "a": { "b": [b.clone()] } }), want);
            assert_eq!(
                document_to_fields(&doc! { "k": b }),
                Err(InvalidValue::UnsupportedType(name))
            );
        }
    }

    #[test]
    fn a_field_name_is_one_segment_and_dots_are_kept() {
        let f = document_to_fields(&doc! { "a.b": 1, "$x": 2, "": 3 }).unwrap();
        assert_eq!(f.len(), 3);
        assert_eq!(f[0].0.segments(), ["a.b"]);
        assert_eq!(f[1].0.segments(), ["$x"]);
        assert_eq!(f[2].0.segments(), [""]);
    }

    #[test]
    fn markers_read_as_empty_documents_and_unset_as_nothing() {
        assert_eq!(value_to_bson(&Value::Map), Some(Bson::Document(doc! {})));
        assert_eq!(value_to_bson(&Value::ImplicitMap), Some(Bson::Document(doc! {})));
        assert_eq!(value_to_bson(&Value::Unset), None);
        assert_eq!(value_to_document(&Value::Int32(1)), None);
    }
}
