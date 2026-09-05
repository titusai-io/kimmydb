//! What can go wrong, and what a caller may do about it.

use std::fmt;
use std::sync::Arc;

/// A failure, from the wire or from the attempt to reach it.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server refused, and said why in the envelope every route uses.
    #[error("{status} {code}: {message}")]
    Api {
        status: u16,
        code: ErrorCode,
        message: String,
        /// What the server says a client may do about it.
        retry: Retry,
        /// Seconds to wait, from `Retry-After`. Present on `rate_limited`.
        retry_after: Option<u64>,
    },

    /// The request never got an answer: connection refused, a timeout, TLS.
    ///
    /// Carries the endpoint, because with failover the interesting question is
    /// *which* node failed rather than that one did.
    #[error("could not reach {endpoint}: {source}")]
    Transport {
        endpoint: String,
        #[source]
        source: reqwest::Error,
    },

    /// Every endpoint was tried and none answered.
    #[error("no node answered; tried {}", .tried.join(", "))]
    NoNodeAvailable { tried: Vec<String> },

    /// The client holds no token and the request needs one.
    #[error("not authenticated: build the client with credentials or a token")]
    NotAuthenticated,

    /// The server answered with something that is not the shape the protocol
    /// promises. Its own bug, or a server too new to be understood.
    #[error("unexpected response from {endpoint}: {detail}")]
    Protocol { endpoint: String, detail: String },

    #[error("change stream: {0}")]
    Stream(String),
}

impl Error {
    /// What a client may do about it, whatever kind it is.
    ///
    /// A transport failure is `Elsewhere` because that is what it means: this
    /// node did not answer, and a peer holds the same data.
    pub fn retry(&self) -> Retry {
        match self {
            Self::Api { retry, .. } => *retry,
            Self::Transport { .. } => Retry::Elsewhere,
            _ => Retry::No,
        }
    }

    /// The server's code, when there is one.
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Self::Api { code, .. } => Some(code.clone()),
            _ => None,
        }
    }

    /// The string form of the server's code without cloning the code enum.
    pub fn code_str(&self) -> Option<&str> {
        match self {
            Self::Api { code, .. } => Some(code.as_str()),
            _ => None,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Api { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// Whether this is the server saying the caller's credentials are no good.
    pub fn is_unauthorized(&self) -> bool {
        self.code() == Some(ErrorCode::Unauthorized)
    }
}

/// What a client may do about a failure.
///
/// Three-valued because KimmyDB is leaderless: every node accepts writes, so
/// "ask a different node" is a real answer and the right one for a failure
/// local to the node that answered (ADR-057).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retry {
    /// Nothing to retry. The request must change, or the condition must.
    No,
    /// The same node, after a delay.
    Wait,
    /// A different node.
    Elsewhere,
}

impl Retry {
    fn parse(s: &str) -> Self {
        match s {
            "wait" => Self::Wait,
            "elsewhere" => Self::Elsewhere,
            // An unknown class is treated as `no`, which is the safe reading:
            // a client that does not understand the advice does not act on it.
            _ => Self::No,
        }
    }
}

/// A code from the server's error envelope.
///
/// `Unknown` is not a gap — it is how a code added after this client was
/// released stays additive. Branch on [`Retry`] when the code is not one you
/// know; that is what it is for.
///
/// Not `Copy`, because [`ErrorCode::Unknown`] owns the code it was handed. That
/// is the point of the variant: a caller meeting a code newer than its client
/// can still log and report *which* code it was, and a `&'static str` payload
/// cannot hold a string that arrived over a socket. It stores an `Arc<str>` so
/// inspection and cloning remain cheap without reallocating strings.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    NoVectors,
    ResumeTokenExpired,
    RateLimited,
    Internal,
    Misconfigured,
    Snapshot,
    NotImplemented,
    ProviderError,
    /// A conditional write's `if_stamp` did not match. Re-read and decide again.
    Stale,
    /// The request outlived the node's deadline for it and was abandoned. The
    /// class is `wait`: the deadline is only ever reached while the request is
    /// waiting, and neither a slow upload nor a slow provider improves by
    /// moving to a peer.
    Timeout,
    /// A code this client does not know. The string is kept.
    Unknown(Arc<str>),
}

impl ErrorCode {
    fn parse(s: &str) -> Self {
        match s {
            "bad_request" => Self::BadRequest,
            "payload_too_large" => Self::PayloadTooLarge,
            "unsupported_media_type" => Self::UnsupportedMediaType,
            "unauthorized" => Self::Unauthorized,
            "forbidden" => Self::Forbidden,
            "not_found" => Self::NotFound,
            "conflict" => Self::Conflict,
            "duplicate_key" => Self::DuplicateKey,
            "unique_violation" => Self::UniqueViolation,
            "no_vectors" => Self::NoVectors,
            "resume_token_expired" => Self::ResumeTokenExpired,
            "rate_limited" => Self::RateLimited,
            "internal" => Self::Internal,
            "misconfigured" => Self::Misconfigured,
            "snapshot" => Self::Snapshot,
            "not_implemented" => Self::NotImplemented,
            "provider_error" => Self::ProviderError,
            "stale" => Self::Stale,
            "timeout" => Self::Timeout,
            // The code is kept rather than flattened to a placeholder. A
            // caller that meets a code newer than its client can act on
            // `retry` and still say in a log *which* code it was; a client
            // that reports every one of them as "unknown" makes the additive
            // case undiagnosable.
            other => Self::Unknown(Arc::from(other)),
        }
    }

    /// The wire string for this error code.
    pub fn as_str(&self) -> &str {
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
            Self::Timeout => "timeout",
            Self::Unknown(s) => s.as_ref(),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Build an [`Error::Api`] from a refusal.
///
/// The body is the envelope every route uses. A body that is *not* one — a
/// proxy's HTML error page, say — still produces an `Api` error with the
/// status, because the status is the part that came from HTTP and is worth
/// keeping.
pub(crate) fn from_response(
    status: u16,
    retry_after: Option<u64>,
    body: &serde_json::Value,
) -> Error {
    let code = body["error"].as_str().unwrap_or("unknown");
    Error::Api {
        status,
        code: ErrorCode::parse(code),
        message: body["message"].as_str().unwrap_or("(no message)").to_string(),
        // Absent means this server predates the field, so fall back to what
        // the status class implies: 5xx may be worth trying elsewhere, 4xx is
        // the caller's to fix.
        retry: match body["retry"].as_str() {
            Some(s) => Retry::parse(s),
            None if status >= 500 => Retry::Elsewhere,
            None if status == 429 => Retry::Wait,
            None => Retry::No,
        },
        retry_after,
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_refusal_carries_its_code_and_class() {
        let e = from_response(
            409,
            None,
            &json!({ "error": "duplicate_key", "message": "duplicate _id", "retry": "no" }),
        );
        assert_eq!(e.code(), Some(ErrorCode::DuplicateKey));
        assert_eq!(e.retry(), Retry::No);
    }

    #[test]
    fn an_unknown_code_is_not_an_error_in_itself() {
        // How a code added after this client shipped stays additive: the code
        // is unrecognized, and the *class* still tells the client what to do.
        let e = from_response(
            503,
            None,
            &json!({ "error": "shed_load", "message": "busy", "retry": "wait" }),
        );
        assert!(matches!(e.code(), Some(ErrorCode::Unknown(_))));
        assert_eq!(e.retry(), Retry::Wait, "the class is what a client acts on");
    }

    #[test]
    fn an_unknown_code_keeps_the_string_the_server_sent() {
        // The whole use of the variant. `retry` tells the client what to do;
        // the code is what it puts in the log so a human can find out what
        // happened. Flattening every unrecognized code to one placeholder —
        // which this did — makes the additive case indistinguishable from
        // every other additive case.
        let e = from_response(
            503,
            None,
            &json!({ "error": "shed_load", "message": "busy", "retry": "wait" }),
        );
        assert_eq!(e.code(), Some(ErrorCode::Unknown(Arc::from("shed_load"))));
        assert_eq!(e.code_str(), Some("shed_load"));
        assert_eq!(
            e.code().expect("a code").to_string(),
            "shed_load",
            "an unrecognized code must render as itself, not as a placeholder"
        );
        // And it reaches the message a caller prints, which is where it is
        // actually read.
        assert!(e.to_string().contains("shed_load"), "{e}");

        // Two different unrecognized codes stay different, which they cannot
        // be if the payload is a constant.
        let other =
            from_response(503, None, &json!({ "error": "quiesced", "message": "x" })).code();
        assert_ne!(e.code(), other);
    }

    #[test]
    fn a_timeout_is_a_code_this_client_knows() {
        // `503 timeout` is a documented code with a documented class
        // (ADR-099). Without a variant it parsed as `Unknown`, so a caller
        // branching on the code could not see a timeout at all — the `retry`
        // hint carried the behaviour and nothing carried the reason.
        assert_eq!(ErrorCode::parse("timeout"), ErrorCode::Timeout);
        assert_eq!(ErrorCode::Timeout.to_string(), "timeout");

        let e = from_response(
            503,
            None,
            &json!({ "error": "timeout", "message": "the request was not completed in time",
                     "retry": "wait" }),
        );
        assert_eq!(e.code(), Some(ErrorCode::Timeout));
        assert_eq!(e.retry(), Retry::Wait);
    }

    #[test]
    fn a_server_without_the_retry_field_falls_back_to_the_status() {
        // A node older than ADR-057. Guessing from the status is worse advice
        // than the server's own, and better than none.
        let old = from_response(500, None, &json!({ "error": "internal", "message": "x" }));
        assert_eq!(old.retry(), Retry::Elsewhere);
        let old = from_response(400, None, &json!({ "error": "bad_request", "message": "x" }));
        assert_eq!(old.retry(), Retry::No);
    }

    #[test]
    fn every_documented_code_round_trips() {
        // The mutation pass found this table barely exercised: the suite
        // produces three or four codes, so the rest could each have been
        // renamed without anything noticing. They are public surface — a
        // caller matching on `ErrorCode::UniqueViolation` is matching on this
        // — and the strings are the specification's rather than this crate's
        // to choose.
        //
        // Written out rather than imported: this crate depends on no server
        // crate, so the only way it can agree with `docs/openapi.yaml` is by
        // repeating it and being checked. It had fallen two behind the server:
        // `stale` and `timeout` were both parseable-or-not with nothing here
        // to say which, and `timeout` was in fact not.
        const CODES: [&str; 19] = [
            "bad_request",
            "payload_too_large",
            "unsupported_media_type",
            "unauthorized",
            "forbidden",
            "not_found",
            "conflict",
            "duplicate_key",
            "unique_violation",
            "no_vectors",
            "resume_token_expired",
            "rate_limited",
            "internal",
            "misconfigured",
            "snapshot",
            "not_implemented",
            "provider_error",
            "stale",
            "timeout",
        ];

        for code in CODES {
            let parsed = ErrorCode::parse(code);
            assert!(
                !matches!(parsed, ErrorCode::Unknown(_)),
                "{code} is documented but this client does not recognize it"
            );
            assert_eq!(parsed.to_string(), code, "{code} does not render back to itself");
        }

        // And the codes are distinct: two variants rendering the same string
        // would make one of them unmatchable.
        let mut rendered: Vec<String> =
            CODES.iter().map(|c| ErrorCode::parse(c).to_string()).collect();
        rendered.sort();
        rendered.dedup();
        assert_eq!(rendered.len(), CODES.len());
    }

    #[test]
    fn every_retry_class_round_trips() {
        for (text, expected) in
            [("no", Retry::No), ("wait", Retry::Wait), ("elsewhere", Retry::Elsewhere)]
        {
            assert_eq!(Retry::parse(text), expected);
        }
        // Anything else is `no`, which is the safe reading: a client that does
        // not understand the advice does not act on it.
        assert_eq!(Retry::parse("shed-load"), Retry::No);
    }

    #[test]
    fn a_body_that_is_not_the_envelope_still_makes_an_error() {
        // A proxy's HTML page, or a body that never reached the application.
        let e = from_response(502, None, &json!("<html>gateway</html>"));
        assert_eq!(e.status(), Some(502));
        assert_eq!(e.retry(), Retry::Elsewhere);
    }
}
