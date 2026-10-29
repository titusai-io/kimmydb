//! A collection's validator: the `$jsonSchema` a write must satisfy, and what
//! happens when it does not (ADR-257).
//!
//! The schema is held as the document the client sent. It is parsed by
//! `kimmy_query::json_schema::Schema`, which this crate cannot name, when it is
//! set and when the metadata carrying it is loaded; this is only the stored form
//! and the two policies beside it.

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorSpec {
    /// The `$jsonSchema` document itself, not wrapped in `{ "$jsonSchema": .. }`.
    pub schema: Document,
    #[serde(default)]
    pub action: ValidationAction,
    #[serde(default)]
    pub level: ValidationLevel,
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
}
