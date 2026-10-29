//! A collection's validator over HTTP (ADR-257): setting, replacing and removing it,
//! how `describe` shows it, and the report of the documents that fail it.

use axum::Json;
use axum::extract::{Path, State};
use kimmy_core::{ValidationAction, ValidationLevel, ValidatorSpec};
use kimmy_storage::CollectionMeta;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::exec::{self, DEFAULT_LIMIT, MAX_LIMIT};
use crate::json::{JsonBody, QueryParams, bson_to_json, json_to_document};
use crate::state::{Auth, SharedState};
use kimmy_auth::Action;

/// The three fields a request names a validator by, in the create body and the
/// body of `PUT .../validator`.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ValidatorInput {
    #[serde(default)]
    pub validator: Option<Value>,
    #[serde(default, rename = "validationAction")]
    pub validation_action: Option<String>,
    #[serde(default, rename = "validationLevel")]
    pub validation_level: Option<String>,
}

impl ValidatorInput {
    /// The validator the fields describe, or `None` when none was named. A policy
    /// without a validator is refused rather than ignored: it would be a setting
    /// that did nothing.
    pub fn into_spec(self) -> Result<Option<ValidatorSpec>, ApiError> {
        let Some(validator) = self.validator else {
            if self.validation_action.is_some() || self.validation_level.is_some() {
                return Err(ApiError::bad_request(
                    "`validationAction` and `validationLevel` need a `validator` to apply to",
                ));
            }
            return Ok(None);
        };
        let action = match self.validation_action.as_deref() {
            None | Some("error") => ValidationAction::Error,
            Some("warn") => ValidationAction::Warn,
            Some(other) => {
                return Err(ApiError::bad_request(format!(
                    "`validationAction` is `error` or `warn`, not `{other}`"
                )));
            }
        };
        let level = match self.validation_level.as_deref() {
            None | Some("strict") => ValidationLevel::Strict,
            Some("moderate") => ValidationLevel::Moderate,
            Some(other) => {
                return Err(ApiError::bad_request(format!(
                    "`validationLevel` is `strict` or `moderate`, not `{other}`"
                )));
            }
        };
        let mut wrapper = json_to_document(&validator).map_err(|_| only_json_schema())?;
        let schema = match (wrapper.remove("$jsonSchema"), wrapper.is_empty()) {
            (Some(bson::Bson::Document(schema)), true) => schema,
            (Some(_), true) => {
                return Err(ApiError::bad_request("`$jsonSchema` requires a schema document"));
            }
            _ => return Err(only_json_schema()),
        };
        let spec = ValidatorSpec { schema, action, level };
        // The parse a bad schema is refused by, here and not on the first write.
        kimmy_storage::check_validator_spec(&spec)?;
        Ok(Some(spec))
    }
}

fn only_json_schema() -> ApiError {
    ApiError::bad_request(
        "a validator is `{ \"$jsonSchema\": <schema> }`; only `$jsonSchema` is supported",
    )
}

/// How a collection's validator reads in `describe`: the validator as it was set
/// (`null` when there is none) and, beside it, its two policies.
pub fn describe_json(meta: &CollectionMeta) -> Value {
    match &meta.validator {
        None => json!({ "validator": null }),
        Some(spec) => json!({
            "validator": { "$jsonSchema": bson_to_json(&bson::Bson::Document(spec.schema.clone())) },
            "validationAction": spec.action.as_str(),
            "validationLevel": spec.level.as_str(),
        }),
    }
}

pub async fn set_validator(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<ValidatorInput>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Ddl, &db, Some(&coll))?;
    let Some(spec) = body.into_spec()? else {
        return Err(ApiError::bad_request(
            "`validator` is required: send `{ \"$jsonSchema\": <schema> }`, or DELETE the \
             validator to remove it",
        ));
    };
    let meta = kimmy_storage::blocking(|| state.engine.set_validator(&db, &coll, Some(spec)))?;
    let mut body = describe_json(&meta);
    body["collection"] = json!(meta.name);
    Ok(Json(body))
}

pub async fn remove_validator(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Ddl, &db, Some(&coll))?;
    let had = state.engine.get_collection(&db, &coll)?.validator.is_some();
    kimmy_storage::blocking(|| state.engine.set_validator(&db, &coll, None))?;
    // What this call did, not what it asked for: a removal sent again finds nothing to
    // remove.
    Ok(Json(json!({ "removed": had })))
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ViolationsQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

/// `GET .../validator/violations`: the documents that fail the collection's
/// current validator, found by scanning it, one bounded page at a time.
pub async fn validator_violations(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    QueryParams(q): QueryParams<ViolationsQuery>,
) -> Result<Json<Value>, ApiError> {
    let _span = exec::op_span("validator_violations", &db, Some(&coll)).entered();
    let meta = exec::authorize(&state, &auth, Action::Read, &db, &coll)?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let after = match &q.cursor {
        Some(raw) => Some(kimmy_core::Cursor::decode(raw)?),
        None => None,
    };
    let page = kimmy_storage::blocking(|| {
        state.engine.validator_violations(&meta, after.as_ref().map(|c| c.key()), limit)
    })?;
    let rows: Vec<Value> = page
        .rows
        .iter()
        .map(|(id, reason)| json!({ "_id": bson_to_json(id), "reason": reason }))
        .collect();
    let mut body = json!({
        "violations": rows,
        "count": rows.len(),
        "scanned": page.scanned,
        "validator": meta.validator.is_some(),
    });
    if let Some(next) = page.next {
        body["nextCursor"] = json!(kimmy_core::Cursor::from_key(next).encode());
    }
    Ok(Json(body))
}
