//! A collection's validator: the `$jsonSchema` a write must satisfy, and what
//! happens when it does not (ADR-257).
//!
//! The schema is held as the document the client sent. It is parsed by
//! [`crate::json_schema::Schema`] when it is set and when the metadata carrying it
//! is loaded; this is the stored form and the two policies beside it.
//!
//! **The stored form keeps every type** ([`stored_schema`], ADR-182's rule for a
//! partial filter): the collection metadata is JSON, and serde's relaxed form
//! read a generic `Binary` back as an array of integers and a `NaN` or an
//! infinity as `null`, so the schema a member enforced was not the schema the
//! client set (ADR-257).

use bson::Document;
use serde::{Deserialize, Serialize};

/// What a write that fails the schema does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ValidationAction {
    /// The write is refused.
    #[default]
    Error,
    /// The write is accepted, logged and counted.
    Warn,
}

impl ValidationAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
        }
    }
}

/// Which writes are held to the schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ValidationLevel {
    /// Every insert and every update.
    #[default]
    Strict,
    /// Every insert; an update only of a document that conformed before it.
    Moderate,
}

impl ValidationLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Moderate => "moderate",
        }
    }
}

/// A collection's validator, as stored and as it replicates.
///
/// Two specs are equal when their schemas have the same canonical encoding, not
/// when `Document`'s `==` says so, which is `f64`'s: a schema holding a `NaN`
/// (now that one survives storage) would differ from itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorSpec {
    /// The `$jsonSchema` document itself, not wrapped in `{ "$jsonSchema": .. }`.
    #[serde(with = "stored_schema")]
    pub schema: Document,
    #[serde(default)]
    pub action: ValidationAction,
    #[serde(default)]
    pub level: ValidationLevel,
}

impl PartialEq for ValidatorSpec {
    fn eq(&self, other: &Self) -> bool {
        self.action == other.action
            && self.level == other.level
            && stored_schema::canonical(&self.schema) == stored_schema::canonical(&other.schema)
    }
}

impl Eq for ValidatorSpec {}

/// How [`ValidatorSpec::schema`] crosses serde: as [`crate::index_meta::stored_filter`]
/// carries a partial filter (ADR-182).
///
/// **Written** as canonical Extended JSON to a human-readable serializer, the
/// collection metadata's JSON, and as the plain document to any other, which is
/// BSON: a `ConfigureValidator` body and a snapshot page are the bytes they were.
/// **Read** as the plain document, in whichever form it was written.
pub mod stored_schema {
    use bson::Document;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::index_meta::stored_filter;

    pub fn serialize<S: Serializer>(schema: &Document, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() { canonical(schema).serialize(s) } else { schema.serialize(s) }
    }

    /// `schema` as the collection metadata stores it: canonical Extended JSON, every
    /// `NaN` one quiet `NaN`.
    pub fn canonical(schema: &Document) -> serde_json::Value {
        stored_filter::canonical(&Some(schema.clone())).unwrap_or(serde_json::Value::Null)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Document, D::Error> {
        Document::deserialize(d)
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    #[test]
    fn the_policies_default_to_error_and_strict() {
        let spec: ValidatorSpec =
            serde_json::from_str(r#"{ "schema": { "required": ["a"] } }"#).unwrap();
        assert_eq!(spec.action, ValidationAction::Error);
        assert_eq!(spec.level, ValidationLevel::Strict);
    }

    #[test]
    fn a_spec_round_trips_through_json_and_bson() {
        let spec = ValidatorSpec {
            schema: doc! { "bsonType": "object", "required": ["a"], "properties": { "a": { "minimum": 1 } } },
            action: ValidationAction::Warn,
            level: ValidationLevel::Moderate,
        };
        let text = serde_json::to_string(&spec).unwrap();
        assert_eq!(serde_json::from_str::<ValidatorSpec>(&text).unwrap(), spec);
        let bytes = bson::serialize_to_vec(&spec).unwrap();
        assert_eq!(bson::deserialize_from_slice::<ValidatorSpec>(&bytes).unwrap(), spec);
    }

    /// A schema holding a value of every type the relaxed JSON form loses or blurs.
    fn typed_schema() -> Document {
        use bson::{Binary, Bson, Decimal128, spec::BinarySubtype};
        doc! {
            "bsonType": "object",
            "properties": {
                "bin": { "enum": [Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] })] },
                "low": { "minimum": f64::NEG_INFINITY },
                "high": { "maximum": f64::INFINITY },
                "nan": { "enum": [f64::NAN, 1] },
                "wide": { "enum": [Bson::Int64(5), Bson::Int32(5)] },
                "dec": { "multipleOf": "1.50".parse::<Decimal128>().unwrap() },
                "date": { "enum": [bson::DateTime::from_millis(1_700_000_000_123)] },
                "oid": { "enum": [bson::oid::ObjectId::parse_str("0123456789abcdef01234567").unwrap()] },
            }
        }
    }

    #[test]
    fn the_schema_keeps_every_type_through_the_metadata_json() {
        let spec = ValidatorSpec {
            schema: typed_schema(),
            action: ValidationAction::Error,
            level: ValidationLevel::Strict,
        };
        let text = serde_json::to_string(&spec).unwrap();
        let back: ValidatorSpec = serde_json::from_str(&text).unwrap();
        assert_eq!(back, spec);
        // `==` on the documents is `f64`'s, so the types are read out one by one.
        let props = back.schema.get_document("properties").unwrap();
        let member =
            |name: &str, key: &str| props.get_document(name).unwrap().get(key).unwrap().clone();
        assert!(
            matches!(member("bin", "enum"), bson::Bson::Array(a) if matches!(a[0], bson::Bson::Binary(_)))
        );
        assert_eq!(member("low", "minimum"), bson::Bson::Double(f64::NEG_INFINITY));
        assert_eq!(member("high", "maximum"), bson::Bson::Double(f64::INFINITY));
        let bson::Bson::Array(nan) = member("nan", "enum") else { panic!() };
        assert!(matches!(nan[0], bson::Bson::Double(d) if d.is_nan()));
        let bson::Bson::Array(wide) = member("wide", "enum") else { panic!() };
        assert_eq!((&wide[0], &wide[1]), (&bson::Bson::Int64(5), &bson::Bson::Int32(5)));
        assert!(matches!(member("dec", "multipleOf"), bson::Bson::Decimal128(_)));
        let bson::Bson::Array(date) = member("date", "enum") else { panic!() };
        assert!(matches!(date[0], bson::Bson::DateTime(_)));
        let bson::Bson::Array(oid) = member("oid", "enum") else { panic!() };
        assert!(matches!(oid[0], bson::Bson::ObjectId(_)));
    }

    #[test]
    fn the_bson_form_stays_the_plain_document() {
        let spec = ValidatorSpec {
            schema: typed_schema(),
            action: ValidationAction::Warn,
            level: ValidationLevel::Moderate,
        };
        let bytes = bson::serialize_to_vec(&spec).unwrap();
        let raw = bson::RawDocumentBuf::from_bytes(bytes.clone()).unwrap();
        let schema = raw.get("schema").unwrap().unwrap();
        assert!(matches!(schema, bson::raw::RawBsonRef::Document(_)));
        let back: ValidatorSpec = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn a_spec_holding_a_nan_equals_itself() {
        let spec = ValidatorSpec {
            schema: typed_schema(),
            action: ValidationAction::Error,
            level: ValidationLevel::Strict,
        };
        assert_eq!(spec, spec.clone());
        let mut other = spec.clone();
        other.level = ValidationLevel::Moderate;
        assert_ne!(spec, other);
    }
}
