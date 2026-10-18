//! `Idempotency-Key` at the HTTP and MCP edges (B5).
//!
//! A client may send `Idempotency-Key` with a document write. The member that
//! applies the write keeps a record of it in the write's own transaction, and a
//! retry with the same key gets the first answer back, marked
//! `Idempotency-Replayed: ?1`, with nothing written again. The record is the
//! storage engine's; what is here is everything around it at the edge:
//!
//! - **the header** ([`parse_header_value`], [`key_from_headers`]): one field
//!   line holding one RFC 9651 sf-String of 16 to 255 printable ASCII
//!   characters once unescaped, as draft-ietf-httpapi-idempotency-key-header-07
//!   §2.1 requires, and the same rule without quotes for the MCP argument
//!   `idempotency_key` ([`from_raw`]); the rule itself is `kimmy-core`'s
//!   ([`IdempotencyKey`]), which the scope hash and the derived `_id` read;
//! - **the fingerprint** ([`fingerprint`], [`tool_fingerprint`]): what decides
//!   whether a retry is the same request, taken over the request as sent;
//! - **the extractors** ([`KeyedJson`], [`KeyedNoBody`]) a keyed route reads its
//!   body and its key through, which keep every refusal `JsonBody` makes;
//! - **the answer** ([`WriteAnswer`], [`mark_replayed`]) and the outcomes
//!   `kimmy_idempotency_requests_total` counts ([`KeyedOutcome`]). The two log
//!   lines a replay and a resume write are the engine's, written where it finds
//!   the record, with the record's own origin.
//!
//! Which routes take the header is decided before any of this runs, before
//! authentication, by the layer over the route table
//! (`routes::IDEMPOTENT_ROUTES`), which refuses a malformed key too. Whether
//! this member takes keys at all now (the gate,
//! [`crate::AppState::idempotency_gate`]) is asked here, after authentication,
//! by the extractors and by [`tool_key`]: its refusal can name a member of the
//! cluster. This module refuses a malformed key on its own as well, so a
//! handler is never handed one, whoever built its router.
//!
//! What the key does once admitted is the executor's: `crate::exec`'s write
//! functions hand it to the engine's keyed twins with the principal's scope,
//! and render a fresh answer and a replay through one function each
//! (`crate::outcome`).

use std::fmt;

use axum::body::{Body, Bytes};
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, FromRequestParts, MatchedPath, Request};
use axum::http::header::{HeaderMap, HeaderName, HeaderValue};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::ApiError;
use crate::json::JsonBody;
use crate::state::{AppState, SharedState};

/// The gate's types, from the cluster layer that answers it, for an embedder
/// that installs a predicate of its own ([`AppState::set_idempotency_gate`]).
pub use kimmy_cluster::{IdempotencyClosed, IdempotencyGate, IdempotencyPredicate};

/// The request header (draft-07 §2.1), as `http` spells header names.
pub const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");

/// The response header that marks a replay: KimmyDB's own, since the draft
/// defines no marker and without one a client cannot tell a replay from a
/// second application on a member that lacked the record.
pub const IDEMPOTENCY_REPLAYED: HeaderName = HeaderName::from_static("idempotency-replayed");

/// `?1`, the sf-Boolean true (RFC 9651 §3.3.6): the only value the marker has.
pub const REPLAYED: HeaderValue = HeaderValue::from_static("?1");

/// The MCP write tools' argument, which carries the key where a header cannot:
/// an MCP client sets no header per tool call.
pub const MCP_ARGUMENT: &str = "idempotency_key";

/// The shortest key taken, in characters of the unescaped value: `kimmy-core`'s
/// bound, named here for the refusal that publishes it.
pub const MIN_KEY_CHARS: usize = kimmy_core::idempotency::KEY_MIN_LEN;

/// The longest key taken, in characters of the unescaped value.
pub const MAX_KEY_CHARS: usize = kimmy_core::idempotency::KEY_MAX_LEN;

/// How long a key is remembered when nothing has said otherwise:
/// `storage.oplog_retention_secs`'s default, 24 hours, the engine's own.
pub const DEFAULT_WINDOW_SECS: u64 = kimmy_storage::idempotency::DEFAULT_WINDOW_SECS;

/// A key as the client meant it, unquoted and unescaped, held to the published
/// format: `kimmy-core`'s, which the scope hash and the derived `_id` read too.
/// Not stored anywhere, and never printed.
pub use kimmy_core::{Fingerprint, IdempotencyKey, KeyError};

/// Why a key was refused. Each is a `400 bad_request` naming the header (or
/// the MCP argument) and this reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRefusal {
    /// The header came on more than one field line: a field-line rule, so the
    /// edge's own, where the value's rule is `kimmy-core`'s.
    Repeated,
    /// The value broke the published format ([`IdempotencyKey`]).
    Invalid(KeyError),
}

impl fmt::Display for KeyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repeated => f.write_str("it was sent on more than one field line"),
            Self::Invalid(e) => fmt::Display::fmt(e, f),
        }
    }
}

/// Read one field line's value as the key it carries, by `kimmy-core`'s rule
/// ([`IdempotencyKey::parse_header_value`]): an sf-String (RFC 9651 §3.3.3)
/// with no parameters, whose unescaped value is [`MIN_KEY_CHARS`] to
/// [`MAX_KEY_CHARS`] printable ASCII characters. A value that is not UTF-8 at
/// all (an obs-text byte `http` admits) holds a character outside that
/// alphabet, and is refused as one.
pub fn parse_header_value(value: &[u8]) -> Result<IdempotencyKey, KeyRefusal> {
    let text =
        std::str::from_utf8(value).map_err(|_| KeyRefusal::Invalid(KeyError::BadCharacter))?;
    IdempotencyKey::parse_header_value(text).map_err(KeyRefusal::Invalid)
}

/// The key the MCP argument `idempotency_key` carries: the same rule as the
/// header, without the quotes, so `"` and `\` are characters like any other
/// and `ab"c` here is the key `"ab\"c"` is there
/// ([`IdempotencyKey::from_raw`]).
pub fn from_raw(text: &str) -> Result<IdempotencyKey, KeyRefusal> {
    IdempotencyKey::from_raw(text).map_err(KeyRefusal::Invalid)
}

/// The `400` a malformed header draws, naming the header, the reason and the
/// published format (draft-07 §5).
pub fn header_refused(refusal: KeyRefusal) -> ApiError {
    ApiError::bad_request(format!(
        "Idempotency-Key: {refusal}. It must be one quoted string (RFC 9651) of \
         {MIN_KEY_CHARS} to {MAX_KEY_CHARS} printable ASCII characters, sent once, such as a \
         UUID: Idempotency-Key: \"6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90\""
    ))
}

/// The `400` a malformed MCP argument draws.
pub fn argument_refused(refusal: KeyRefusal) -> ApiError {
    ApiError::bad_request(format!(
        "{MCP_ARGUMENT}: {refusal}. It must be {MIN_KEY_CHARS} to {MAX_KEY_CHARS} printable \
         ASCII characters, such as a UUID"
    ))
}

/// The key a request carries, if any: `None` without the header, the key with
/// one well-formed field line, and the `400` naming the header otherwise.
pub fn key_from_headers(headers: &HeaderMap) -> Result<Option<IdempotencyKey>, ApiError> {
    let mut lines = headers.get_all(IDEMPOTENCY_KEY).iter();
    let Some(first) = lines.next() else {
        return Ok(None);
    };
    if lines.next().is_some() {
        return Err(header_refused(KeyRefusal::Repeated));
    }
    parse_header_value(first.as_bytes()).map(Some).map_err(header_refused)
}

/// The `400` a key draws on `update` or `delete` with `explain: true`, which
/// plans the write and performs none, so there is nothing for a key to make
/// happen once.
pub fn refused_with_explain() -> ApiError {
    ApiError::bad_request(
        "`explain: true` writes nothing, so it does not take an Idempotency-Key; send the \
         request without the header",
    )
}

// ---------------------------------------------------------------------------
// The fingerprint
// ---------------------------------------------------------------------------

/// The domain tags the two fingerprints begin with, so neither can equal the
/// other or any other SHA-256 this build takes over a request.
const HTTP_TAG: &[u8] = b"kimmydb idempotency fingerprint http v1\0";
const TOOL_TAG: &[u8] = b"kimmydb idempotency fingerprint mcp v1\0";

/// What decides whether a keyed retry is the same request (draft-07 §2.4,
/// "checksum of the entire request payload"): SHA-256 over the method, the
/// route template, the path and query string as sent, and the body bytes as
/// sent. Each part is preceded by its length as eight big-endian bytes, so no
/// two different requests concatenate to the same input.
///
/// As sent, not as parsed: a body whose keys come in another order, or a path
/// percent-encoded differently, is another request, which a retry of the same
/// request never is. The route template is in it so that one path cannot be
/// read as two routes' requests.
pub fn fingerprint(method: &str, route: &str, path_and_query: &str, body: &[u8]) -> Fingerprint {
    let mut hash = Sha256::new();
    hash.update(HTTP_TAG);
    for part in [method.as_bytes(), route.as_bytes(), path_and_query.as_bytes(), body] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    Fingerprint(hash.finalize().into())
}

/// An MCP tool call's fingerprint: SHA-256 over the tool's name and its
/// arguments without `idempotency_key`, each length-prefixed as in
/// [`fingerprint`].
///
/// **The canonical form.** MCP hands a tool its arguments parsed, so there are
/// no bytes as sent to hash. The arguments are the tool's typed argument
/// struct serialized back to compact JSON: fields in the struct's declaration
/// order, an absent optional left out, a defaulted field at its value (so
/// `"multi": false` and no `multi` are one request, as they are one request),
/// and every JSON value inside (a document, a filter) with its keys in the
/// order the client sent them and its numbers as `serde_json` renders the
/// parsed value. A top-level `idempotency_key` member is removed first, so the
/// key itself is never part of the request it names.
pub fn tool_fingerprint(tool: &str, arguments: &Value) -> Fingerprint {
    let mut arguments = arguments.clone();
    if let Value::Object(members) = &mut arguments {
        members.shift_remove(MCP_ARGUMENT);
    }
    let arguments = serde_json::to_vec(&arguments).unwrap_or_default();
    let mut hash = Sha256::new();
    hash.update(TOOL_TAG);
    for part in [tool.as_bytes(), arguments.as_slice()] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    Fingerprint(hash.finalize().into())
}

/// A keyed request, as the edge hands it on: the key, and the fingerprint of
/// the request it came with. The principal, which completes its scope, is the
/// caller's to add once authorization has named it ([`Self::keyed_write`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotentRequest {
    pub key: IdempotencyKey,
    pub fingerprint: Fingerprint,
}

impl IdempotentRequest {
    /// What the engine keys the write by, for the principal whose
    /// [`AppState::principal_scope`] is `principal`: the key's scope hash under
    /// that principal, and this request's fingerprint. The collection, the
    /// record's other half, is the engine's.
    pub fn keyed_write(&self, principal: &str) -> kimmy_storage::KeyedWrite {
        kimmy_storage::KeyedWrite::new(self.key.scope_hash(principal), self.fingerprint)
    }
}

/// The route template the router matched, which the fingerprint and the guard
/// both read. Empty for a request no router routed, which only a test makes.
fn route_of(extensions: &axum::http::Extensions) -> String {
    extensions.get::<MatchedPath>().map(|m| m.as_str().to_string()).unwrap_or_default()
}

fn target_of(uri: &axum::http::Uri) -> &str {
    uri.path_and_query().map_or("", |pq| pq.as_str())
}

/// A keyed write route's JSON body, and the key it came with.
///
/// Without the header this is exactly [`JsonBody`]: the same extractor, the
/// same refusals, and the same body. With it, the gate is asked first: a member
/// that does not take keys now refuses with `400` naming the cause, after
/// authentication (the handler's `Auth` runs before this) and before a byte of
/// the body is read. Then the body is read as bytes through
/// axum's `Bytes` extractor, hashed into the fingerprint, and then parsed, and
/// every refusal `JsonBody` makes is made the same way, in the same order:
///
/// - `415 unsupported_media_type` for a body that is not declared JSON, by
///   axum's own check, before a byte of it is read;
/// - `413 payload_too_large` past `server.max_body_bytes`, which the `Bytes`
///   extractor honours as `Json` does (`DefaultBodyLimit`), with the refused
///   body handed back to `limits::drain_refused_body` as it is today;
/// - `400` for JSON that does not parse and `422` for JSON of the wrong shape,
///   by `Json`'s own parse of the same bytes.
pub struct KeyedJson<T> {
    pub body: T,
    pub key: Option<IdempotentRequest>,
}

impl<T> FromRequest<SharedState> for KeyedJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &SharedState) -> Result<Self, Self::Rejection> {
        let Some(key) = key_from_headers(req.headers())? else {
            let JsonBody(body) = JsonBody::<T>::from_request(req, state).await?;
            return Ok(Self { body, key: None });
        };
        refuse_unless_open(state, |closed| closed.to_string())?;
        if let Some(refused) = refused_content_type(req.headers()).await {
            return Err(refused);
        }
        let method = req.method().as_str().to_string();
        let route = route_of(req.extensions());
        let target = target_of(req.uri()).to_string();
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|rejection| ApiError::from(JsonRejection::from(rejection)))?;
        let fingerprint = fingerprint(&method, &route, &target, &bytes);
        let axum::Json(body) = axum::Json::<T>::from_bytes(&bytes)?;
        Ok(Self { body, key: Some(IdempotentRequest { key, fingerprint }) })
    }
}

/// `Json`'s `415`, decided by `Json` itself: the headers alone, handed to the
/// extractor with an empty body, are refused for their content type before
/// the body is looked at, and anything else it says about the empty body is
/// not this request's answer. So a keyed request is refused for its content
/// type exactly when an unkeyed one is, with axum's own words, and the rule is
/// never copied here to drift from axum's.
async fn refused_content_type(headers: &HeaderMap) -> Option<ApiError> {
    let mut probe = Request::new(Body::empty());
    *probe.headers_mut() = headers.clone();
    match axum::Json::<serde::de::IgnoredAny>::from_request(probe, &()).await {
        Err(rejection @ JsonRejection::MissingJsonContentType(_)) => Some(rejection.into()),
        _ => None,
    }
}

/// The key a route with no body came with: `DELETE …/docs/{id}`, whose
/// fingerprint is taken over an empty body whatever was sent, since the route
/// reads none. The gate is asked as [`KeyedJson`] asks it.
pub struct KeyedNoBody(pub Option<IdempotentRequest>);

impl FromRequestParts<SharedState> for KeyedNoBody {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &SharedState,
    ) -> Result<Self, Self::Rejection> {
        let Some(key) = key_from_headers(&parts.headers)? else {
            return Ok(Self(None));
        };
        refuse_unless_open(state, |closed| closed.to_string())?;
        let fingerprint = fingerprint(
            parts.method.as_str(),
            &route_of(&parts.extensions),
            target_of(&parts.uri),
            b"",
        );
        Ok(Self(Some(IdempotentRequest { key, fingerprint })))
    }
}

/// Refuse a well-formed key while this member does not take keys (the gate:
/// the operator's switch and the live-peer net), with `400 bad_request` and the
/// gate's own words, which name the switch or the live member; counted as
/// `kimmy_idempotency_requests_total{outcome="gated"}`. Asked after
/// authentication, since the refusal can name a member of the cluster, and
/// before anything is read, looked up or written.
fn refuse_unless_open(
    state: &AppState,
    message: impl FnOnce(&IdempotencyClosed) -> String,
) -> Result<(), ApiError> {
    match state.idempotency_gate() {
        Ok(()) => Ok(()),
        Err(closed) => {
            state.metrics.record_idempotency(KeyedOutcome::Gated);
            Err(ApiError::bad_request(message(&closed)))
        }
    }
}

/// The MCP write tools' key: `None` without the argument, the key and the
/// call's fingerprint with a well-formed one on a member whose gate is open,
/// and the `400` the header would draw otherwise. The argument is checked
/// before the gate, as the header is, so a malformed key is named whatever the
/// member's state.
pub fn tool_key(
    state: &AppState,
    tool: &str,
    key: Option<&str>,
    arguments: &impl serde::Serialize,
) -> Result<Option<IdempotentRequest>, ApiError> {
    let Some(key) = key else {
        return Ok(None);
    };
    let key = from_raw(key).map_err(argument_refused)?;
    refuse_unless_open(state, |closed| format!("{MCP_ARGUMENT}: {closed}"))?;
    let arguments = serde_json::to_value(arguments)
        .map_err(|e| ApiError::internal(format!("the tool's arguments did not serialize: {e}")))?;
    Ok(Some(IdempotentRequest { key, fingerprint: tool_fingerprint(tool, &arguments) }))
}

// ---------------------------------------------------------------------------
// The answer
// ---------------------------------------------------------------------------

/// Mark a response as a replay: `Idempotency-Replayed: ?1`.
pub fn mark_replayed(headers: &mut HeaderMap) {
    headers.insert(IDEMPOTENCY_REPLAYED, REPLAYED);
}

/// A keyed route's `200`: the body an outcome rendered, and whether it is the
/// first answer or a replay of it. A replay is the same body, status and all,
/// with [`IDEMPOTENCY_REPLAYED`]; nothing else about the response differs.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteAnswer {
    pub body: Value,
    pub replayed: bool,
}

impl WriteAnswer {
    /// The answer of a request that was processed now.
    pub fn first(body: Value) -> Self {
        Self { body, replayed: false }
    }

    /// The first answer, returned again from the record.
    pub fn replay(body: Value) -> Self {
        Self { body, replayed: true }
    }
}

impl IntoResponse for WriteAnswer {
    fn into_response(self) -> Response {
        let mut response = axum::Json(self.body).into_response();
        if self.replayed {
            mark_replayed(response.headers_mut());
        }
        response
    }
}

/// What became of a keyed request, the label of
/// `kimmy_idempotency_requests_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyedOutcome {
    /// No record: processed, and its record written if it changed anything.
    Applied,
    /// Answered from the record.
    Replayed,
    /// A multi-document request that had stopped, continued.
    Resumed,
    /// `422 idempotency_key_reused`.
    Reused,
    /// `409 idempotency_key_in_flight`.
    InFlight,
    /// `400` by the gate: the switch is off here, or a live member does not
    /// carry records.
    Gated,
}

impl KeyedOutcome {
    pub const ALL: [Self; 6] =
        [Self::Applied, Self::Replayed, Self::Resumed, Self::Reused, Self::InFlight, Self::Gated];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Replayed => "replayed",
            Self::Resumed => "resumed",
            Self::Reused => "reused",
            Self::InFlight => "in_flight",
            Self::Gated => "gated",
        }
    }

    /// Its position in [`Self::ALL`].
    pub const fn slot(self) -> usize {
        self as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str) -> Result<String, KeyRefusal> {
        parse_header_value(text.as_bytes()).map(|k| k.as_str().to_string())
    }

    fn invalid<T>(e: KeyError) -> Result<T, KeyRefusal> {
        Err(KeyRefusal::Invalid(e))
    }

    const UUID: &str = "6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90";

    // The header as this edge reads it. The rule is `kimmy-core`'s, and its own
    // tests are the rule's specification; these hold the edge to it through the
    // bytes a field line arrives as. Accepted: a quoted UUID; the bounds, 16 and
    // 255, counted on the unescaped value; the two escapes; a space inside;
    // whitespace around the Item. Refused: an empty value, a bare token, an
    // unclosed string, any other escape, a character outside printable ASCII (a
    // non-ASCII byte, a control character, a tab, a byte that is not UTF-8), 15
    // and 256 characters, parameters, anything else after the string, a list
    // included, and a second field line.

    #[test]
    fn a_quoted_key_is_its_unquoted_value() {
        assert_eq!(key(&format!("\"{UUID}\"")), Ok(UUID.to_string()));
        assert_eq!(key("\"key with a space in it\""), Ok("key with a space in it".to_string()));
        assert_eq!(key(&format!("  \"{UUID}\"  ")), Ok(UUID.to_string()), "SP around the Item");
    }

    #[test]
    fn the_length_is_the_unescaped_values_and_its_bounds_are_16_and_255() {
        let of = |n: usize| format!("\"{}\"", "k".repeat(n));
        assert_eq!(key(&of(15)), invalid(KeyError::Length { found: 15 }));
        assert_eq!(key(&of(16)).map(|k| k.len()), Ok(16));
        assert_eq!(key(&of(255)).map(|k| k.len()), Ok(255));
        assert_eq!(key(&of(256)), invalid(KeyError::Length { found: 256 }));
        assert_eq!(key("\"\""), invalid(KeyError::Length { found: 0 }));

        // 14 plain characters and two escapes are 16 once unescaped, and 18 as
        // sent: the unescaped count is the one that decides.
        let escaped = format!("\"{}\\\"\\\\\"", "k".repeat(14));
        assert_eq!(escaped.len() - 2, 18);
        assert_eq!(key(&escaped), Ok(format!("{}\"\\", "k".repeat(14))));
        // 254 plain and one escape: 255 unescaped, 256 as sent.
        let at_the_top = format!("\"{}\\\\\"", "k".repeat(254));
        assert_eq!(key(&at_the_top).map(|k| k.len()), Ok(255));
        let over = format!("\"{}\\\\\"", "k".repeat(255));
        assert_eq!(key(&over), invalid(KeyError::Length { found: 256 }));
    }

    #[test]
    fn only_a_quote_and_a_backslash_are_escaped() {
        assert_eq!(key(&format!("\"{UUID}\\n\"")), invalid(KeyError::BadEscape));
        assert_eq!(key(&format!("\"{UUID}\\")), invalid(KeyError::Unterminated));
    }

    #[test]
    fn a_value_that_is_not_one_quoted_string_is_refused() {
        assert_eq!(key(""), invalid(KeyError::NotAString));
        assert_eq!(key("   "), invalid(KeyError::NotAString));
        assert_eq!(key(UUID), invalid(KeyError::NotAString), "a bare token is not a String");
        assert_eq!(key("12345678901234567"), invalid(KeyError::NotAString));
        assert_eq!(key(&format!("\"{UUID}")), invalid(KeyError::Unterminated));
        assert_eq!(key(&format!("\"{UUID}\";a=1")), invalid(KeyError::Parameters));
        assert_eq!(key(&format!("\"{UUID}\";a")), invalid(KeyError::Parameters));
        assert_eq!(key(&format!("\"{UUID}\" ;a=1")), invalid(KeyError::Parameters));
        assert_eq!(key(&format!("\"{UUID}\", \"{UUID}\"")), invalid(KeyError::TrailingInput));
        assert_eq!(key(&format!("\"{UUID}\"x")), invalid(KeyError::TrailingInput));
    }

    #[test]
    fn a_character_outside_printable_ascii_is_refused() {
        for bad in ["é", "\u{7f}", "\t", "\u{1}"] {
            assert_eq!(
                key(&format!("\"{UUID}{bad}\"")),
                invalid(KeyError::BadCharacter),
                "{bad:?}"
            );
        }
        // An obs-text byte, which `HeaderValue` admits and a `&str` cannot hold.
        let mut raw = format!("\"{UUID}").into_bytes();
        raw.extend_from_slice(&[0xff, b'"']);
        assert_eq!(parse_header_value(&raw), invalid(KeyError::BadCharacter));
    }

    #[test]
    fn the_mcp_argument_is_the_same_rule_without_quotes() {
        assert_eq!(from_raw(UUID).map(|k| k.as_str().to_string()), Ok(UUID.to_string()));
        // The characters a header must escape are plain here, and name the same key.
        let raw = format!("{}\"\\", "k".repeat(14));
        assert_eq!(
            from_raw(&raw),
            parse_header_value(format!("\"{}\\\"\\\\\"", "k".repeat(14)).as_bytes())
        );
        assert_eq!(from_raw(&"k".repeat(15)), invalid(KeyError::Length { found: 15 }));
        assert_eq!(from_raw(&"k".repeat(256)), invalid(KeyError::Length { found: 256 }));
        assert_eq!(from_raw(""), invalid(KeyError::Length { found: 0 }));
        assert_eq!(from_raw(&format!("{UUID}é")), invalid(KeyError::BadCharacter));
        assert_eq!(from_raw(&format!("{UUID}\n")), invalid(KeyError::BadCharacter));
    }

    #[test]
    fn the_header_is_read_from_one_field_line_and_refused_by_name() {
        let mut headers = HeaderMap::new();
        assert_eq!(key_from_headers(&headers).unwrap(), None);
        headers.append(IDEMPOTENCY_KEY, HeaderValue::from_str(&format!("\"{UUID}\"")).unwrap());
        assert_eq!(
            key_from_headers(&headers).unwrap().map(|k| k.as_str().to_string()),
            Some(UUID.into())
        );
        // The same value on a second line is still two lines.
        headers.append(IDEMPOTENCY_KEY, HeaderValue::from_str(&format!("\"{UUID}\"")).unwrap());
        let refused = key_from_headers(&headers).unwrap_err();
        assert_eq!(refused.status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(refused.code, crate::error::ErrorCode::BadRequest);
        assert!(refused.message.starts_with("Idempotency-Key: "), "{}", refused.message);
        assert!(refused.message.contains("more than one field line"), "{}", refused.message);

        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY, HeaderValue::from_static("short"));
        let refused = key_from_headers(&headers).unwrap_err();
        assert!(refused.message.starts_with("Idempotency-Key: "), "{}", refused.message);
        assert!(
            refused.message.contains("16 to 255"),
            "the format is published: {}",
            refused.message
        );
    }

    #[test]
    fn every_refusal_says_why_in_words() {
        for refusal in [
            KeyRefusal::Repeated,
            KeyRefusal::Invalid(KeyError::NotAString),
            KeyRefusal::Invalid(KeyError::Unterminated),
            KeyRefusal::Invalid(KeyError::BadEscape),
            KeyRefusal::Invalid(KeyError::BadCharacter),
            KeyRefusal::Invalid(KeyError::Parameters),
            KeyRefusal::Invalid(KeyError::TrailingInput),
            KeyRefusal::Invalid(KeyError::Length { found: 0 }),
            KeyRefusal::Invalid(KeyError::Length { found: 300 }),
        ] {
            let header = header_refused(refusal.clone()).message;
            assert!(header.contains(&refusal.to_string()), "{header}");
            assert!(header.starts_with("Idempotency-Key: "), "{header}");
            let argument = argument_refused(refusal).message;
            assert!(argument.starts_with("idempotency_key: "), "{argument}");
        }
    }

    /// The edge's bounds are `kimmy-core`'s, so the refusal publishes the rule
    /// the engine's scope hash and derived `_id` are taken under.
    #[test]
    fn the_published_bounds_are_the_cores() {
        assert_eq!((MIN_KEY_CHARS, MAX_KEY_CHARS), (16, 255));
    }

    #[test]
    fn the_fingerprint_moves_with_every_part_of_the_request_as_sent() {
        let route = "/v1/db/{db}/coll/{coll}/docs";
        let base = fingerprint("POST", route, "/v1/db/shop/coll/orders/docs", br#"{"a":1,"b":2}"#);
        assert_eq!(
            base,
            fingerprint("POST", route, "/v1/db/shop/coll/orders/docs", br#"{"a":1,"b":2}"#),
            "the same request, the same fingerprint"
        );
        let differs = [
            fingerprint("POST", route, "/v1/db/shop/coll/orders/docs", br#"{"a":1,"b":3}"#),
            // The same JSON value with its keys in another order is another
            // request: the bytes as sent, not the value parsed.
            fingerprint("POST", route, "/v1/db/shop/coll/orders/docs", br#"{"b":2,"a":1}"#),
            fingerprint("POST", route, "/v1/db/shop/coll/orders/docs", br#"{"a":1, "b":2}"#),
            fingerprint("POST", route, "/v1/db/shop/coll/carts/docs", br#"{"a":1,"b":2}"#),
            fingerprint("POST", route, "/v1/db/shop/coll/orders/docs?x=1", br#"{"a":1,"b":2}"#),
            fingerprint("PUT", route, "/v1/db/shop/coll/orders/docs", br#"{"a":1,"b":2}"#),
            fingerprint(
                "POST",
                "/v1/db/{db}/coll/{coll}/bulk",
                "/v1/db/shop/coll/orders/docs",
                br#"{"a":1,"b":2}"#,
            ),
        ];
        for (i, other) in differs.iter().enumerate() {
            assert_ne!(&base, other, "case {i}");
        }
        // Length-prefixed, so a byte moved from one part to the next is another
        // request, not the same concatenation.
        assert_ne!(fingerprint("POST", "ab", "c", b""), fingerprint("POST", "a", "bc", b""));
        assert_ne!(fingerprint("POST", "a", "", b"b"), fingerprint("POST", "a", "b", b""));
    }

    /// The fingerprints' bytes are a protocol constant: a derived `_id` is
    /// built on them, so a release that hashed a keyed retry differently (the
    /// tag, the layout, a route template) would derive another `_id` for it and
    /// leave two documents. Golden values for the request of each route that
    /// derives an `_id` and each MCP tool form that does, computed outside this
    /// code from the layout the doc comments state.
    #[test]
    fn the_fingerprints_are_pinned_by_golden_vectors() {
        fn hex(fp: Fingerprint) -> String {
            fp.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
        }
        for (route, target, body, expected) in [
            (
                "/v1/db/{db}/coll/{coll}/docs",
                "/v1/db/shop/coll/orders/docs",
                &br#"{"n":1}"#[..],
                "2431ab8d5e91c7978d857d2f8f7f703c6eb74d793e6b4941ec20216680660129",
            ),
            (
                "/v1/db/{db}/coll/{coll}/bulk",
                "/v1/db/shop/coll/orders/bulk",
                &br#"[{"n":1},{"n":2}]"#[..],
                "24684180d8e249bc100a25e4d2bb1cf7c7585ff83f2d9194172c8010a89b806b",
            ),
            (
                "/v1/db/{db}/coll/{coll}/find_and_modify",
                "/v1/db/shop/coll/orders/find_and_modify",
                &br#"{"filter":{"sku":"g"},"update":{"$set":{"q":1}},"upsert":true}"#[..],
                "5d2c93c788ffc8a48147bff52bc03795ab3f3e3d6b5d122f33b1c7d20378896a",
            ),
        ] {
            assert_eq!(hex(fingerprint("POST", route, target, body)), expected, "{route}");
        }
        for (tool, arguments, expected) in [
            (
                "insert",
                serde_json::json!({"database":"shop","collection":"orders","document":{"n":1}}),
                "803ebbf5f4f1b770cfbc7d3b900c2d0599adffa7807179f7d0c22afaadd82e53",
            ),
            (
                "insert",
                serde_json::json!({"database":"sales","collection":"orders","document":{"n":1},
                                   "idempotency_key":"golden-mcp-insert-6f1c0e52"}),
                "d2f19c4e4c32b8ce8e7cd51ffc2df4e2bbc0579a3d8cd971ca9ea00f9016298d",
            ),
            (
                "insert_many",
                serde_json::json!({"database":"sales","collection":"orders",
                                   "documents":[{"n":1},{"n":2}],
                                   "idempotency_key":"golden-mcp-many-6f1c0e52"}),
                "73594820f198969f5a7e239ba7666fed5b649c49330095f116201aa0ae708014",
            ),
        ] {
            assert_eq!(hex(tool_fingerprint(tool, &arguments)), expected, "{tool}");
        }
    }

    #[test]
    fn one_body_byte_moves_the_fingerprint() {
        let body = br#"{"_id":"x","n":12345}"#.to_vec();
        let base = fingerprint("POST", "r", "/p", &body);
        for i in 0..body.len() {
            let mut changed = body.clone();
            changed[i] ^= 1;
            assert_ne!(base, fingerprint("POST", "r", "/p", &changed), "byte {i}");
        }
        let mut longer = body.clone();
        longer.push(b' ');
        assert_ne!(base, fingerprint("POST", "r", "/p", &longer), "a trailing byte");
    }

    #[test]
    fn a_tool_fingerprint_is_the_name_and_the_arguments_without_the_key() {
        let call = serde_json::json!({
            "database": "shop", "collection": "orders", "document": { "a": 1, "b": 2 },
            "idempotency_key": UUID,
        });
        let base = tool_fingerprint("insert", &call);
        let mut other_key = call.clone();
        other_key["idempotency_key"] = Value::from("another-key-of-sixteen");
        assert_eq!(base, tool_fingerprint("insert", &other_key), "the key is not the request");
        let mut without = call.clone();
        without.as_object_mut().unwrap().shift_remove("idempotency_key");
        assert_eq!(base, tool_fingerprint("insert", &without));

        assert_ne!(base, tool_fingerprint("insert_many", &call), "the tool is part of it");
        let mut changed = call.clone();
        changed["document"]["b"] = Value::from(3);
        assert_ne!(base, tool_fingerprint("insert", &changed));
        // Key order inside a document is part of the request, as it is part of
        // what is stored.
        let reordered = serde_json::json!({
            "database": "shop", "collection": "orders", "document": { "b": 2, "a": 1 },
        });
        assert_ne!(base, tool_fingerprint("insert", &reordered));
        assert_ne!(base, fingerprint("insert", "", "", &serde_json::to_vec(&without).unwrap()));
    }

    #[test]
    fn a_replay_is_the_same_body_with_the_marker() {
        let body = serde_json::json!({ "insertedId": 1, "stamp": "s" });
        let first = WriteAnswer::first(body.clone()).into_response();
        assert_eq!(first.status(), axum::http::StatusCode::OK);
        assert!(first.headers().get(IDEMPOTENCY_REPLAYED).is_none());
        let replay = WriteAnswer::replay(body).into_response();
        assert_eq!(replay.status(), axum::http::StatusCode::OK);
        assert_eq!(replay.headers().get("Idempotency-Replayed").unwrap(), "?1");
        assert_eq!(
            first.headers().get(axum::http::header::CONTENT_TYPE),
            replay.headers().get(axum::http::header::CONTENT_TYPE)
        );
    }

    #[test]
    fn the_outcome_labels_are_the_documented_six_in_slot_order() {
        let labels: Vec<_> = KeyedOutcome::ALL.iter().map(|o| o.label()).collect();
        assert_eq!(labels, ["applied", "replayed", "resumed", "reused", "in_flight", "gated"]);
        for (i, outcome) in KeyedOutcome::ALL.iter().enumerate() {
            assert_eq!(outcome.slot(), i);
        }
    }
}
