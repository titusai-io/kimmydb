//! JSON ⇄ BSON conversion at the HTTP edge.
//!
//! Documents are stored as BSON because Mongo-style comparison needs typed
//! values and a defined cross-type order. JSON cannot express several of those
//! types, so the boundary uses MongoDB's Extended JSON v2 conventions:
//! `{"$oid": "..."}`, `{"$date": ...}`, `{"$numberLong": "..."}`.
//!
//! Plain JSON still works for everything expressible in it, so a caller who
//! does not care about the distinction never has to see this.

use bson::{Bson, Document};
use serde_json::{Map, Value};

use crate::error::ApiError;

/// A JSON request body whose refusals are the API's error envelope.
///
/// Axum's `Json<T>` rejects a malformed or wrong-shaped body with bare text and
/// no `error` code. `From<JsonRejection> for ApiError` has existed since M5 to
/// fix that — but a handler only reaches it by taking `Result<Json<T>, _>`, and
/// exactly one of nineteen did. The other eighteen answered `422` in
/// `text/plain`, outside the taxonomy entirely, which is not something a
/// specification can be written about honestly.
///
/// So the mapping lives in the extractor rather than in a shape each handler
/// has to remember to write: `JsonBody<T>` cannot be used without it. Found by
/// driving a real node, because every conformance scenario that exercised a
/// wrong-shaped body happened to use the one route that had it right.
pub struct JsonBody<T>(pub T);

impl<T, S> axum::extract::FromRequest<S> for JsonBody<T>
where
    axum::Json<T>:
        axum::extract::FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let axum::Json(value) = axum::Json::<T>::from_request(req, state).await?;
        Ok(Self(value))
    }
}

/// Convert a JSON value into BSON, honouring Extended JSON wrappers.
pub fn json_to_bson(value: &Value) -> Result<Bson, ApiError> {
    Ok(match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::String(s) => Bson::String(s.clone()),
        Value::Number(n) => number_to_bson(n)?,
        Value::Array(items) => {
            Bson::Array(items.iter().map(json_to_bson).collect::<Result<_, _>>()?)
        }
        Value::Object(map) => {
            if let Some(bson) = extended_json(map)? {
                bson
            } else {
                let mut doc = Document::new();
                for (key, value) in map {
                    doc.insert(key.clone(), json_to_bson(value)?);
                }
                Bson::Document(doc)
            }
        }
    })
}

/// JSON numbers carry no type, so integers stay integers and everything else
/// becomes a double. Widening whole numbers to f64 would break `$type` queries
/// and silently lose precision above 2^53.
fn number_to_bson(n: &serde_json::Number) -> Result<Bson, ApiError> {
    if let Some(i) = n.as_i64() {
        return Ok(if i >= i64::from(i32::MIN) && i <= i64::from(i32::MAX) {
            Bson::Int32(i as i32)
        } else {
            Bson::Int64(i)
        });
    }
    match n.as_f64() {
        Some(f) => Ok(Bson::Double(f)),
        None => Err(ApiError::bad_request(format!("number {n} is not representable"))),
    }
}

/// Recognize an Extended JSON wrapper object.
fn extended_json(map: &Map<String, Value>) -> Result<Option<Bson>, ApiError> {
    if map.len() != 1 {
        return Ok(None);
    }
    let (key, value) = map.iter().next().expect("length checked");

    let bad = |what: &str| ApiError::bad_request(format!("invalid {what}: {value}"));

    Ok(Some(match key.as_str() {
        "$oid" => {
            let s = value.as_str().ok_or_else(|| bad("$oid"))?;
            Bson::ObjectId(s.parse().map_err(|_| bad("$oid"))?)
        }
        "$date" => match value {
            // Milliseconds since the epoch, or an RFC 3339 string.
            Value::Number(n) => {
                Bson::DateTime(bson::DateTime::from_millis(n.as_i64().ok_or_else(|| bad("$date"))?))
            }
            Value::String(s) => {
                Bson::DateTime(bson::DateTime::parse_rfc3339_str(s).map_err(|_| bad("$date"))?)
            }
            Value::Object(inner) => {
                let ms = inner
                    .get("$numberLong")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or_else(|| bad("$date"))?;
                Bson::DateTime(bson::DateTime::from_millis(ms))
            }
            _ => return Err(bad("$date")),
        },
        "$numberLong" => {
            let s = value.as_str().ok_or_else(|| bad("$numberLong"))?;
            Bson::Int64(s.parse().map_err(|_| bad("$numberLong"))?)
        }
        "$numberInt" => {
            let s = value.as_str().ok_or_else(|| bad("$numberInt"))?;
            Bson::Int32(s.parse().map_err(|_| bad("$numberInt"))?)
        }
        "$numberDouble" => {
            let s = value.as_str().ok_or_else(|| bad("$numberDouble"))?;
            Bson::Double(s.parse().map_err(|_| bad("$numberDouble"))?)
        }
        "$binary" => {
            use base64::Engine as _;
            let inner = value.as_object().ok_or_else(|| bad("$binary"))?;
            let b64 = inner.get("base64").and_then(Value::as_str).ok_or_else(|| bad("$binary"))?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|_| bad("$binary"))?;
            Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes })
        }
        "$minKey" => Bson::MinKey,
        "$maxKey" => Bson::MaxKey,
        _ => return Ok(None),
    }))
}

/// Convert BSON back to JSON, emitting Extended JSON for types JSON lacks.
pub fn bson_to_json(value: &Bson) -> Value {
    match value {
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::String(s) | Bson::Symbol(s) => Value::String(s.clone()),
        Bson::Int32(i) => Value::from(*i),
        Bson::Int64(i) => Value::from(*i),
        Bson::Double(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            // NaN and the infinities have no JSON form; Extended JSON spells
            // them as strings rather than silently becoming null.
            .unwrap_or_else(|| serde_json::json!({ "$numberDouble": f.to_string() })),
        Bson::Array(items) => Value::Array(items.iter().map(bson_to_json).collect()),
        Bson::Document(doc) => document_to_json(doc),
        Bson::ObjectId(oid) => serde_json::json!({ "$oid": oid.to_hex() }),
        Bson::DateTime(dt) => serde_json::json!({ "$date": dt.timestamp_millis() }),
        Bson::Binary(b) => {
            use base64::Engine as _;
            serde_json::json!({
                "$binary": {
                    "base64": base64::engine::general_purpose::STANDARD.encode(&b.bytes),
                    "subType": format!("{:02x}", u8::from(b.subtype)),
                }
            })
        }
        Bson::RegularExpression(re) => serde_json::json!({
            "$regularExpression": { "pattern": re.pattern.as_str(), "options": re.options.as_str() }
        }),
        Bson::Timestamp(ts) => {
            serde_json::json!({ "$timestamp": { "t": ts.time, "i": ts.increment } })
        }
        Bson::Decimal128(d) => serde_json::json!({ "$numberDecimal": d.to_string() }),
        Bson::MinKey => serde_json::json!({ "$minKey": 1 }),
        Bson::MaxKey => serde_json::json!({ "$maxKey": 1 }),
        Bson::JavaScriptCode(c) => serde_json::json!({ "$code": c }),
        Bson::JavaScriptCodeWithScope(c) => {
            serde_json::json!({ "$code": c.code, "$scope": document_to_json(&c.scope) })
        }
        Bson::DbPointer(_) => Value::Null,
    }
}

pub fn document_to_json(doc: &Document) -> Value {
    let mut map = Map::new();
    for (key, value) in doc {
        map.insert(key.clone(), bson_to_json(value));
    }
    Value::Object(map)
}

/// Convert a JSON object into a BSON document, rejecting anything else.
pub fn json_to_document(value: &Value) -> Result<Document, ApiError> {
    match json_to_bson(value)? {
        Bson::Document(doc) => Ok(doc),
        _ => Err(ApiError::bad_request("expected a JSON object")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn round_trip(value: Value) -> Value {
        bson_to_json(&json_to_bson(&value).unwrap())
    }

    #[test]
    fn plain_json_round_trips() {
        let v = json!({ "s": "text", "b": true, "n": null, "a": [1, 2, 3] });
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn whole_numbers_stay_integers() {
        // Widening to f64 would break $type queries and lose precision above
        // 2^53.
        assert_eq!(json_to_bson(&json!(42)).unwrap(), Bson::Int32(42));
        assert_eq!(json_to_bson(&json!(3_000_000_000i64)).unwrap(), Bson::Int64(3_000_000_000));
        assert_eq!(json_to_bson(&json!(1.5)).unwrap(), Bson::Double(1.5));
    }

    #[test]
    fn large_integers_survive_the_round_trip_exactly() {
        let big = 9_007_199_254_740_993i64;
        assert_eq!(json_to_bson(&json!(big)).unwrap(), Bson::Int64(big));
        assert_eq!(round_trip(json!(big)), json!(big));
    }

    #[test]
    fn object_ids_use_extended_json() {
        let oid = bson::oid::ObjectId::new();
        let value = json!({ "$oid": oid.to_hex() });
        assert_eq!(json_to_bson(&value).unwrap(), Bson::ObjectId(oid));
        assert_eq!(round_trip(value.clone()), value);
    }

    #[test]
    fn dates_accept_millis_and_rfc3339() {
        assert_eq!(
            json_to_bson(&json!({ "$date": 1_700_000_000_000i64 })).unwrap(),
            Bson::DateTime(bson::DateTime::from_millis(1_700_000_000_000))
        );
        let parsed = json_to_bson(&json!({ "$date": "2023-11-14T22:13:20Z" })).unwrap();
        assert!(matches!(parsed, Bson::DateTime(_)));
    }

    #[test]
    fn explicit_number_wrappers_are_honoured() {
        assert_eq!(
            json_to_bson(&json!({ "$numberLong": "9007199254740993" })).unwrap(),
            Bson::Int64(9_007_199_254_740_993)
        );
        assert_eq!(json_to_bson(&json!({ "$numberInt": "7" })).unwrap(), Bson::Int32(7));
    }

    #[test]
    fn binary_round_trips() {
        let value = json!({ "$binary": { "base64": "AQIDBA==", "subType": "00" } });
        let bson = json_to_bson(&value).unwrap();
        assert!(matches!(&bson, Bson::Binary(b) if b.bytes == vec![1, 2, 3, 4]));
        assert_eq!(round_trip(value.clone()), value);
    }

    #[test]
    fn a_single_key_object_that_is_not_a_wrapper_stays_a_document() {
        // `{"$oid": ...}` is special; `{"total": 1}` must not be.
        let v = json!({ "total": 1 });
        assert_eq!(round_trip(v.clone()), v);
        // An unknown $-prefixed key is a plain field, not a failed wrapper.
        let v = json!({ "$unknown": 1 });
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn a_malformed_wrapper_is_an_error_rather_than_a_silent_document() {
        assert!(json_to_bson(&json!({ "$oid": "not-hex" })).is_err());
        assert!(json_to_bson(&json!({ "$numberLong": "abc" })).is_err());
        assert!(json_to_bson(&json!({ "$date": true })).is_err());
    }

    #[test]
    fn non_finite_doubles_do_not_become_null() {
        // JSON has no NaN; emitting null would turn a number into a missing
        // value on the way out.
        let out = bson_to_json(&Bson::Double(f64::NAN));
        assert_eq!(out, json!({ "$numberDouble": "NaN" }));
        let out = bson_to_json(&Bson::Double(f64::INFINITY));
        assert_eq!(out, json!({ "$numberDouble": "inf" }));
    }

    #[test]
    fn nested_wrappers_convert_inside_documents_and_arrays() {
        let oid = bson::oid::ObjectId::new();
        let value = json!({ "items": [ { "id": { "$oid": oid.to_hex() } } ] });
        let doc = json_to_document(&value).unwrap();
        let items = doc.get_array("items").unwrap();
        let first = items[0].as_document().unwrap();
        assert_eq!(first.get("id"), Some(&Bson::ObjectId(oid)));
    }

    #[test]
    fn non_objects_are_rejected_where_a_document_is_required() {
        assert!(json_to_document(&json!([1, 2])).is_err());
        assert!(json_to_document(&json!("text")).is_err());
    }
}
