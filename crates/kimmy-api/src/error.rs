//! HTTP error mapping.
//!
//! Every failure becomes a JSON body with a stable `error` code, so clients can
//! branch on something other than prose.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use kimmy_auth::AuthError;
use kimmy_core::Error as CoreError;
use kimmy_storage::StorageError;
use serde_json::json;
use tracing::error;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    /// Seconds to wait before retrying, emitted as a `Retry-After` header.
    ///
    /// Carried on the error rather than assembled at the call site so that a
    /// 429 cannot be returned without one: a refusal that does not say when to
    /// come back leaves a client to guess, and clients guess badly.
    pub retry_after_secs: Option<u64>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into(), retry_after_secs: None }
    }

    /// Over a rate limit.
    ///
    /// The message names no user and no limit: it is returned before
    /// authentication, so anything specific to the attempt would be readable by
    /// whoever triggered it.
    pub fn too_many_requests(retry_after_secs: u64) -> Self {
        Self {
            retry_after_secs: Some(retry_after_secs),
            ..Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many requests; retry later",
            )
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    /// Denied by RBAC.
    ///
    /// Deliberately identical whether or not the target exists: a distinct 404
    /// would let a caller probe for collections they cannot access.
    pub fn forbidden() -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", "not authorized for this operation")
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            error!(code = self.code, message = %self.message, "request failed");
        }
        let body = json!({ "error": self.code, "message": self.message });
        let mut response = (self.status, Json(body)).into_response();
        if let Some(secs) = self.retry_after_secs {
            match axum::http::HeaderValue::from_str(&secs.to_string()) {
                Ok(value) => {
                    response.headers_mut().insert(axum::http::header::RETRY_AFTER, value);
                }
                // A digit string is always a valid header value, so this cannot
                // happen — but dropping the header is better than panicking on
                // the error path.
                Err(e) => error!(error = %e, "could not encode Retry-After"),
            }
        }
        response
    }
}

/// A body axum could not turn into the expected type.
///
/// Axum's own rejection renders bare text with no `error` code, which would
/// make this the one route a client cannot branch on. The status axum chose is
/// kept — it already distinguishes a syntax error from a body that is too
/// large — and only the envelope is made to match every other route.
impl From<axum::extract::rejection::JsonRejection> for ApiError {
    fn from(rejection: axum::extract::rejection::JsonRejection) -> Self {
        let status = rejection.status();
        let code = match status {
            StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
            StatusCode::UNSUPPORTED_MEDIA_TYPE => "unsupported_media_type",
            _ => "bad_request",
        };
        Self::new(status, code, rejection.body_text())
    }
}

impl From<CoreError> for ApiError {
    fn from(e: CoreError) -> Self {
        match &e {
            CoreError::CollectionNotFound { .. }
            | CoreError::DatabaseNotFound(_)
            | CoreError::DocumentNotFound(_) => ApiError::not_found(e.to_string()),
            CoreError::CollectionExists { .. } => ApiError::conflict(e.to_string()),
            CoreError::DuplicateKey(_) => {
                ApiError::new(StatusCode::CONFLICT, "duplicate_key", e.to_string())
            }
            CoreError::UniqueViolation { .. } => {
                ApiError::new(StatusCode::CONFLICT, "unique_violation", e.to_string())
            }
            // Reserved-but-unbuilt capability. 501 says "this will exist";
            // 400 would wrongly imply the caller made a mistake.
            CoreError::Unsupported(_) => {
                ApiError::new(StatusCode::NOT_IMPLEMENTED, "not_implemented", e.to_string())
            }
            CoreError::InvalidName { .. }
            | CoreError::InvalidQuery(_)
            | CoreError::InvalidUpdate(_)
            | CoreError::InvalidDocumentId { .. }
            | CoreError::UnsupportedOperator(_)
            | CoreError::MalformedResumeToken => ApiError::bad_request(e.to_string()),
            CoreError::ResumeTokenExpired => {
                ApiError::new(StatusCode::GONE, "resume_token_expired", e.to_string())
            }
            CoreError::Bson(_) | CoreError::Serialization(_) => ApiError::internal(e.to_string()),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::Core(inner) => inner.into(),
            // Storage-level failures are the server's fault, not the caller's,
            // and their text can name on-disk internals, so it is logged rather
            // than returned.
            other => {
                error!(error = %other, "storage failure");
                ApiError::internal("storage failure")
            }
        }
    }
}

impl From<AuthError> for ApiError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::InvalidCredentials => ApiError::unauthorized("invalid username or password"),
            AuthError::InvalidToken | AuthError::TokenExpired => {
                ApiError::unauthorized(e.to_string())
            }
            AuthError::Forbidden { .. } => ApiError::forbidden(),
            AuthError::UserNotFound(_) => ApiError::not_found(e.to_string()),
            AuthError::UserExists(_) => ApiError::conflict(e.to_string()),
            AuthError::WeakSecret { .. } => ApiError::bad_request(e.to_string()),
            AuthError::Hashing(_) | AuthError::TokenIssue(_) => {
                error!(error = %e, "auth failure");
                ApiError::internal("authentication failure")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_and_conflict_map_to_their_status_codes() {
        let e: ApiError =
            CoreError::CollectionNotFound { db: "a".into(), collection: "b".into() }.into();
        assert_eq!(e.status, StatusCode::NOT_FOUND);

        let e: ApiError = CoreError::DuplicateKey("1".into()).into();
        assert_eq!(e.status, StatusCode::CONFLICT);
        assert_eq!(e.code, "duplicate_key");
    }

    #[test]
    fn a_bad_query_is_the_callers_fault() {
        let e: ApiError = CoreError::InvalidQuery("nope".into()).into();
        assert_eq!(e.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn an_expired_resume_token_is_gone_not_a_generic_error() {
        // 410 tells a client to resubscribe rather than retry the same token.
        let e: ApiError = CoreError::ResumeTokenExpired.into();
        assert_eq!(e.status, StatusCode::GONE);
        assert_eq!(e.code, "resume_token_expired");
    }

    #[test]
    fn storage_internals_are_not_leaked_to_the_caller() {
        let e: ApiError = StorageError::Database("/var/lib/kimmy/kimmy.redb page 42".into()).into();
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!e.message.contains("/var/lib"), "internal paths must not reach the client");
    }

    #[test]
    fn credential_failures_do_not_distinguish_the_cause() {
        let e: ApiError = AuthError::InvalidCredentials.into();
        assert_eq!(e.status, StatusCode::UNAUTHORIZED);
        assert!(!e.message.to_lowercase().contains("user not found"));
    }

    #[test]
    fn forbidden_carries_no_detail_about_the_target() {
        // A message naming the collection would let a caller probe for objects
        // they cannot access.
        let e = ApiError::forbidden();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
        assert!(!e.message.contains("collection"));
    }
}
