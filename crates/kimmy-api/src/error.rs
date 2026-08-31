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
    /// A conditional write found the document at a different version than
    /// the caller's `if_stamp`, or found no document where one was expected.
    Stale,
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
    pub const ALL: [ErrorCode; 18] = [
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
        Self::Stale,
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
            Self::Stale => "stale",
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
            | Self::ResumeTokenExpired
            // The document moved on. The client re-reads, decides again, and
            // sends a *different* request carrying the new stamp; repeating
            // this one can only fail the same way.
            | Self::Stale => Retry::No,

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
    /// A retry hint that differs from the code's usual one. The one case so
    /// far: a collection this node does not have on a node that has peers,
    /// where "elsewhere" is the truth and "no" would tell a client that just
    /// created it to give up.
    pub retry_override: Option<Retry>,
    /// A more specific `error_description` for the `WWW-Authenticate`
    /// challenge than the generic one every 401 carries.
    ///
    /// Carried here and handed to the challenge layer as a response extension
    /// rather than written into the header directly, because that layer is
    /// the one place that also knows the `resource_metadata` pointer — an
    /// error that set the header itself would win the description and lose
    /// the pointer. The one case today is a federated token refused for its
    /// lifetime (ADR-096), where "invalid token" would send a client to
    /// refresh a token the provider will mint identically.
    pub challenge_description: Option<String>,
}

/// The `error_description` a refusal asked for, riding on the response so the
/// challenge layer can use it. See [`ApiError::challenge_description`].
#[derive(Clone, Debug)]
pub struct ChallengeDescription(pub String);

impl ApiError {
    pub fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retry_after_secs: None,
            retry_override: None,
            challenge_description: None,
        }
    }

    /// The same error, with a specific `error_description` in its challenge.
    pub fn with_challenge_description(mut self, description: impl Into<String>) -> Self {
        self.challenge_description = Some(description.into());
        self
    }

    /// The same error with a different retry hint.
    pub fn with_retry(mut self, retry: Retry) -> Self {
        self.retry_override = Some(retry);
        self
    }

    /// The retry hint a client will see.
    pub fn retry(&self) -> Retry {
        self.retry_override.unwrap_or_else(|| self.code.retry())
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

    /// A conditional write refused because the document is not at the
    /// expected version. Names the current stamp so a client that wants to
    /// can skip the re-read — but the honest loop is read, decide, write.
    pub fn stale(current: Option<kimmy_core::Stamp>) -> Self {
        let message = match current {
            Some(stamp) => format!(
                "the document is at stamp {} rather than the one `if_stamp` named; \
                 re-read it and retry with the current stamp",
                stamp.encode()
            ),
            None => "no live document is at the stamp `if_stamp` named; it was deleted \
                     or never existed"
                .to_string(),
        };
        Self::new(StatusCode::CONFLICT, ErrorCode::Stale, message)
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
            "retry": self.retry().as_str(),
        });
        let mut response = (self.status, Json(body)).into_response();
        if let Some(description) = self.challenge_description {
            response.extensions_mut().insert(ChallengeDescription(description));
        }
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
            CoreError::CollectionExists { .. } | CoreError::IndexExists { .. } => {
                ApiError::conflict(e.to_string())
            }
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
            | CoreError::MalformedCursor
            | CoreError::MalformedStamp => ApiError::bad_request(e.to_string()),
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
            // The caller's condition did not hold. Theirs to act on, and the
            // current stamp is the one thing they need to act.
            StorageError::Stale { current } => ApiError::stale(current),
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
            // The refusal names this node's limit and nothing about the token,
            // and it reaches the challenge as well as the body: a client told
            // only `invalid_token` would refresh, and the provider would mint
            // the same token again. The fix is on the provider's side or in
            // this node's configuration, and the description says so
            // (ADR-096).
            AuthError::TokenLifetimeExceeded { .. } | AuthError::TokenLifetimeUnbounded { .. } => {
                ApiError::unauthorized(e.to_string()).with_challenge_description(e.to_string())
            }
            // Reported to the caller as an ordinary invalid token, with the
            // key id kept out of the message. Which signing keys this node
            // has fetched is not something an unauthenticated caller should
            // learn by guessing, and the caller has nothing to do with the
            // answer anyway — the refetch it triggers is this node's job.
            AuthError::UnknownSigningKey(_) => {
                ApiError::unauthorized("authentication token is invalid")
            }
            AuthError::Forbidden { .. } => ApiError::forbidden(),
            AuthError::UserNotFound(_) | AuthError::RoleNotFound(_) => {
                ApiError::not_found(e.to_string())
            }
            AuthError::UserExists(_) | AuthError::RoleExists(_) => {
                ApiError::conflict(e.to_string())
            }
            // Both are configuration refusals raised before the server ever
            // serves, so neither can reach a request. Mapped rather than
            // matched loosely so that adding a variant stays a compile error
            // here instead of silently becoming a 400.
            AuthError::WeakSecret { .. }
            | AuthError::AdminNotFederatable { .. }
            | AuthError::EmptyRoleMapping { .. }
            | AuthError::InvalidResourceIdentifier { .. }
            | AuthError::InvalidTokenLifetimeLimit { .. } => ApiError::bad_request(e.to_string()),
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
    fn a_lifetime_refusal_is_a_401_whose_challenge_names_the_limit() {
        // Both the body and the challenge say which limit and how long, so the
        // fix is discoverable from the response; neither says anything about
        // the token itself (ADR-096).
        for e in [
            AuthError::TokenLifetimeExceeded { max_secs: 900 },
            AuthError::TokenLifetimeUnbounded { max_secs: 900 },
        ] {
            let e: ApiError = e.into();
            assert_eq!(e.status, StatusCode::UNAUTHORIZED);
            assert_eq!(e.code, ErrorCode::Unauthorized);
            assert!(e.message.contains("900 seconds"), "{}", e.message);
            let description = e.challenge_description.as_deref().expect("a specific description");
            assert!(description.contains("900 seconds"), "{description}");
            assert!(description.contains("max_token_lifetime_secs"), "{description}");
        }
        // The ordinary refusals keep the generic challenge.
        let plain: ApiError = AuthError::TokenExpired.into();
        assert!(plain.challenge_description.is_none());
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
