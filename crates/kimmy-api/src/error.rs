//! HTTP error mapping.
//!
//! Every failure becomes a JSON body with a stable `error` code, so clients can
//! branch on something other than prose.
//!
//! # The code set is closed, and the compiler is what closes it
//!
//! [`ErrorCode`] is an enum rather than a `&'static str`, so a new failure
//! cannot invent an eighteenth code at a call site. The wire string and the
//! retry class both come from exhaustive matches on it: adding a variant does
//! not compile until both are answered, which is the point — the second one is
//! a decision about client behaviour that would otherwise be made by accident.
//!
//! The set had already drifted before this existed. `no_vectors` is returned
//! from `vectors.rs` and appeared in neither the HTTP reference nor the first
//! draft of the protocol specification, because both were assembled by reading
//! this file, and the codes accrete across five modules.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use kimmy_auth::AuthError;
use kimmy_core::Error as CoreError;
use kimmy_storage::StorageError;
use serde_json::json;
use tracing::error;

/// Every code the API can return, and nothing else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    PayloadTooLarge,
    UnsupportedMediaType,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    DuplicateKey,
    UniqueViolation,
    /// Searching a collection whose vectors were never ingested.
    NoVectors,
    ResumeTokenExpired,
    RateLimited,
    Internal,
    Misconfigured,
    Snapshot,
    NotImplemented,
    ProviderError,
}

/// What a client may do about a failure.
///
/// Three-valued rather than a boolean because KimmyDB is leaderless. Every
/// node accepts writes, so "ask a different node" is an answer available here
/// that a primary-based database cannot give — and it is the *right* answer
/// for a node-local failure, where telling a client "retryable" would have it
/// hammer the one machine that just failed. See ADR-057.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retry {
    /// Nothing to retry: the request must change, or the condition must.
    No,
    /// The same node, after a delay. `Retry-After` gives the delay when the
    /// server knows it; otherwise the client backs off on its own.
    Wait,
    /// A different node. The failure is local to this one, and a peer holds
    /// the same data — replication is what makes this worth trying.
    Elsewhere,
}

impl Retry {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::No => "no",
            Self::Wait => "wait",
            Self::Elsewhere => "elsewhere",
        }
    }
}

impl ErrorCode {
    /// Every variant, for the tests that hold the specification to this set.
    pub const ALL: [ErrorCode; 17] = [
        Self::BadRequest,
        Self::PayloadTooLarge,
        Self::UnsupportedMediaType,
        Self::Unauthorized,
        Self::Forbidden,
        Self::NotFound,
        Self::Conflict,
        Self::DuplicateKey,
        Self::UniqueViolation,
        Self::NoVectors,
        Self::ResumeTokenExpired,
        Self::RateLimited,
        Self::Internal,
        Self::Misconfigured,
        Self::Snapshot,
        Self::NotImplemented,
        Self::ProviderError,
    ];

    /// The string on the wire. Stable: clients branch on it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::PayloadTooLarge => "payload_too_large",
            Self::UnsupportedMediaType => "unsupported_media_type",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::DuplicateKey => "duplicate_key",
            Self::UniqueViolation => "unique_violation",
            Self::NoVectors => "no_vectors",
            Self::ResumeTokenExpired => "resume_token_expired",
            Self::RateLimited => "rate_limited",
            Self::Internal => "internal",
            Self::Misconfigured => "misconfigured",
            Self::Snapshot => "snapshot",
            Self::NotImplemented => "not_implemented",
            Self::ProviderError => "provider_error",
        }
    }

    /// What a client may do about it.
    pub fn retry(self) -> Retry {
        match self {
            // The request is wrong, or the state it asks about is. Sending it
            // again changes nothing.
            Self::BadRequest
            | Self::PayloadTooLarge
            | Self::UnsupportedMediaType
            | Self::Unauthorized
            | Self::Forbidden
            | Self::NotFound
            | Self::Conflict
            | Self::DuplicateKey
            | Self::UniqueViolation
            // Ingest vectors, or configure a provider that produces them.
            | Self::NoVectors
            // The resume point is collected. Resubscribing is a new request,
            // not a retry of this one, and a client that retries the same
            // token loops forever.
            | Self::ResumeTokenExpired => Retry::No,

            // `not_implemented` has two sources and takes the conservative
            // answer. `CoreError::Unsupported` is a capability that exists
            // nowhere, so retrying anywhere is futile; a node built without
            // `local-embeddings` would be recoverable elsewhere, but only in a
            // cluster built inconsistently, which is not a shape to optimize
            // for. Declaring it `Elsewhere` would send every client round the
            // whole cluster for an answer that will not change.
            Self::NotImplemented => Retry::No,

            Self::RateLimited => Retry::Wait,
            // An upstream embedding provider failed. Every node calls the same
            // provider, so moving does not help; waiting might.
            Self::ProviderError => Retry::Wait,

            // Local to this node, and replication means a peer can answer.
            // A storage failure here says nothing about the peer's disk, and
            // a missing API key or a bad snapshot is this node's own state.
            Self::Internal | Self::Misconfigured | Self::Snapshot => Retry::Elsewhere,
        }
    }
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: ErrorCode,
    pub message: String,
    /// Seconds to wait before retrying, emitted as a `Retry-After` header.
    ///
    /// Carried on the error rather than assembled at the call site so that a
    /// 429 cannot be returned without one: a refusal that does not say when to
    /// come back leaves a client to guess, and clients guess badly.
    pub retry_after_secs: Option<u64>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
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
                ErrorCode::RateLimited,
                "too many requests; retry later",
            )
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, ErrorCode::BadRequest, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, ErrorCode::NotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, ErrorCode::Conflict, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, message)
    }

    /// Denied by RBAC.
    ///
    /// Deliberately identical whether or not the target exists: a distinct 404
    /// would let a caller probe for collections they cannot access.
    pub fn forbidden() -> Self {
        Self::new(StatusCode::FORBIDDEN, ErrorCode::Forbidden, "not authorized for this operation")
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            error!(code = self.code.as_str(), message = %self.message, "request failed");
        }
        // `retry` rides in the envelope rather than living only in the
        // specification, so a client meeting a code added after it was written
        // still knows what to do with it. That is what makes a new code an
        // additive change rather than one that needs every client updated.
        let body = json!({
            "error": self.code.as_str(),
            "message": self.message,
            "retry": self.code.retry().as_str(),
        });
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
            StatusCode::PAYLOAD_TOO_LARGE => ErrorCode::PayloadTooLarge,
            StatusCode::UNSUPPORTED_MEDIA_TYPE => ErrorCode::UnsupportedMediaType,
            _ => ErrorCode::BadRequest,
        };
        Self::new(status, code, rejection.body_text())
    }
}

/// A request to `/watch` that is not a WebSocket upgrade.
///
/// Same reasoning as the JSON rejection above, and found the same way — by
/// driving it. Axum answers `400 Connection header did not include 'upgrade'`
/// as bare text, which made the change-stream route the one place a client
/// meets a refusal it cannot branch on. The status axum chose is kept; only
/// the envelope is made to match every other route.
impl From<axum::extract::ws::rejection::WebSocketUpgradeRejection> for ApiError {
    fn from(rejection: axum::extract::ws::rejection::WebSocketUpgradeRejection) -> Self {
        let status = rejection.status();
        // 426 is the one that is not a client mistake in the usual sense — the
        // connection cannot be upgraded at all — but it is still the caller's
        // to fix, so both take `bad_request`.
        Self::new(status, ErrorCode::BadRequest, rejection.body_text())
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
                ApiError::new(StatusCode::CONFLICT, ErrorCode::DuplicateKey, e.to_string())
            }
            CoreError::UniqueViolation { .. } => {
                ApiError::new(StatusCode::CONFLICT, ErrorCode::UniqueViolation, e.to_string())
            }
            // Reserved-but-unbuilt capability. 501 says "this will exist";
            // 400 would wrongly imply the caller made a mistake.
            CoreError::Unsupported(_) => {
                ApiError::new(StatusCode::NOT_IMPLEMENTED, ErrorCode::NotImplemented, e.to_string())
            }
            CoreError::InvalidName { .. }
            | CoreError::InvalidQuery(_)
            | CoreError::InvalidUpdate(_)
            | CoreError::InvalidDocumentId { .. }
            | CoreError::UnsupportedOperator(_)
            | CoreError::MalformedResumeToken
            | CoreError::MalformedCursor => ApiError::bad_request(e.to_string()),
            CoreError::ResumeTokenExpired => {
                ApiError::new(StatusCode::GONE, ErrorCode::ResumeTokenExpired, e.to_string())
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

    #[tokio::test]
    async fn a_body_rejection_keeps_its_status_and_gains_a_stable_code() {
        // Axum's own rejection renders bare text, so without this mapping the
        // bulk route would be the one endpoint a client cannot branch on. The
        // status axum chose is kept — it already distinguishes a syntax error
        // from a body that is too large — and only the envelope is made to
        // match every other route.
        //
        // Driven through the real extractor rather than over a socket, because
        // the test client always sends a JSON content type and 415 cannot be
        // reached through it.
        use axum::extract::FromRequest;

        let no_content_type = axum::http::Request::builder()
            .method("POST")
            .body(axum::body::Body::from("[]"))
            .unwrap();
        let rejection = axum::Json::<Vec<serde_json::Value>>::from_request(no_content_type, &())
            .await
            .expect_err("a body with no JSON content type must be rejected");
        let mapped: ApiError = rejection.into();
        assert_eq!(mapped.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(mapped.code, ErrorCode::UnsupportedMediaType);

        // And the ordinary case still lands on the generic code.
        let malformed = axum::http::Request::builder()
            .method("POST")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{not json"))
            .unwrap();
        let rejection = axum::Json::<Vec<serde_json::Value>>::from_request(malformed, &())
            .await
            .expect_err("malformed JSON must be rejected");
        assert_eq!(ApiError::from(rejection).code, ErrorCode::BadRequest);
    }

    #[test]
    fn not_found_and_conflict_map_to_their_status_codes() {
        let e: ApiError =
            CoreError::CollectionNotFound { db: "a".into(), collection: "b".into() }.into();
        assert_eq!(e.status, StatusCode::NOT_FOUND);

        let e: ApiError = CoreError::DuplicateKey("1".into()).into();
        assert_eq!(e.status, StatusCode::CONFLICT);
        assert_eq!(e.code, ErrorCode::DuplicateKey);
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
        assert_eq!(e.code, ErrorCode::ResumeTokenExpired);
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
