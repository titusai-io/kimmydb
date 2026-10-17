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
//!   `idempotency_key` ([`from_raw`]);
//! - **the fingerprint** ([`fingerprint`], [`tool_fingerprint`]): what decides
//!   whether a retry is the same request, taken over the request as sent;
//! - **the extractors** ([`KeyedJson`], [`KeyedNoBody`]) a keyed route reads its
//!   body and its key through, which keep every refusal `JsonBody` makes;
//! - **the answer** ([`WriteAnswer`], [`mark_replayed`]), the two log lines a
//!   replay and a resume write ([`log_replayed`], [`log_resumed`]), and the
//!   outcomes `kimmy_idempotency_requests_total` counts ([`KeyedOutcome`]).
//!
//! Which routes take the header, and whether this member takes it at all now,
//! is decided before any of this runs, by the layer over the route table
//! (`routes::IDEMPOTENT_ROUTES`) and the gate it asks
//! ([`crate::AppState::idempotency_gate`]). This module refuses a malformed
//! key on its own as well, so a handler is never handed one, whoever built its
//! router.
//!
//! Until the storage half lands, a request the layer admits is validated,
//! fingerprinted and then applied as an unkeyed request would be: the key does
//! not yet reach the engine.

use std::fmt;

use axum::body::{Body, Bytes};
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, FromRequestParts, MatchedPath, Request};
use axum::http::header::{HeaderMap, HeaderName, HeaderValue};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::error::ApiError;
use crate::json::JsonBody;
use crate::state::AppState;

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

/// The shortest key taken, in characters of the unescaped value. Long enough
/// that a key is not guessed or collided by accident; a UUID is 36.
pub const MIN_KEY_CHARS: usize = 16;

/// The longest key taken, in characters of the unescaped value.
pub const MAX_KEY_CHARS: usize = 255;

/// How long a key is remembered when nothing has said otherwise:
/// `storage.oplog_retention_secs`'s default, 24 hours.
pub const DEFAULT_WINDOW_SECS: u64 = 24 * 60 * 60;

/// A key as the client meant it: unquoted, unescaped, and checked against the
/// published format. Not stored anywhere: the record is found by a hash over
/// the principal, the collection and this.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RawKey(String);

impl RawKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// Why a key was refused. Each is a `400 bad_request` naming the header (or
/// the MCP argument) and this reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRefusal {
    /// The header came on more than one field line.
    Repeated,
    /// The field value is empty.
    Empty,
    /// The value is not a quoted string: a bare token, a number, a list.
    NotAString,
    /// The opening quote is never closed.
    Unterminated,
    /// A backslash escapes something other than `"` or `\`.
    BadEscape,
    /// A character outside printable ASCII, `0x20` to `0x7E`.
    NotPrintable,
    /// The unescaped value is this many characters, outside
    /// [`MIN_KEY_CHARS`]`..=`[`MAX_KEY_CHARS`].
    Length(usize),
    /// Parameters follow the string (`"k";a=1`): refused, not ignored.
    Parameters,
    /// Something other than parameters follows the string.
    Trailing,
}

impl fmt::Display for KeyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repeated => f.write_str("it was sent on more than one field line"),
            Self::Empty => f.write_str("its value is empty"),
            Self::NotAString => {
                f.write_str("its value is not a quoted string (a bare token is not one)")
            }
            Self::Unterminated => f.write_str("its quoted string is not closed"),
            Self::BadEscape => {
                f.write_str(r#"a backslash in it escapes something other than \" or \\"#)
            }
            Self::NotPrintable => f.write_str("it holds a character that is not printable ASCII"),
            Self::Length(0) => f.write_str("the key is empty"),
            Self::Length(n) => write!(
                f,
                "the key is {n} characters once unescaped, and it must be {MIN_KEY_CHARS} to \
                 {MAX_KEY_CHARS}"
            ),
            Self::Parameters => f.write_str("parameters follow the string, and none are taken"),
            Self::Trailing => {
                f.write_str("something follows the quoted string; the value is one string")
            }
        }
    }
}

/// Read one field line's value as the key it carries: an sf-String (RFC 9651
/// §3.3.3, parsed as §4.2 parses an Item), with no parameters, whose unescaped
/// value is [`MIN_KEY_CHARS`] to [`MAX_KEY_CHARS`] characters of the
/// sf-String alphabet. The length is the unescaped value's, so `\"` and `\\`
/// each count as one character, as they will in the scope hash.
///
/// The published rule lives in `kimmy-core`, beside the scope hash and the
/// derived `_id` that use the same unescaped value; integrating the storage
/// half replaces this body with a call to its validator, so both edges and the
/// engine read one definition. Until then this is that rule, and the tests
/// below are its specification.
pub fn parse_header_value(value: &[u8]) -> Result<RawKey, KeyRefusal> {
    // §4.2: leading spaces are discarded, then the Item, then trailing spaces.
    // `http` has already removed the field's surrounding whitespace, so this
    // matters only to a caller handing in raw bytes.
    let start = value.iter().position(|&b| b != b' ').unwrap_or(value.len());
    let input = &value[start..];
    let Some((&first, mut rest)) = input.split_first() else {
        return Err(KeyRefusal::Empty);
    };
    if first != b'"' {
        return Err(KeyRefusal::NotAString);
    }
    let mut key = String::new();
    loop {
        let Some((&byte, after)) = rest.split_first() else {
            return Err(KeyRefusal::Unterminated);
        };
        rest = after;
        match byte {
            b'"' => break,
            b'\\' => {
                let Some((&escaped, after)) = rest.split_first() else {
                    return Err(KeyRefusal::Unterminated);
                };
                if escaped != b'"' && escaped != b'\\' {
                    return Err(KeyRefusal::BadEscape);
                }
                key.push(char::from(escaped));
                rest = after;
            }
            0x20..=0x7e => key.push(char::from(byte)),
            _ => return Err(KeyRefusal::NotPrintable),
        }
    }
    if rest.first() == Some(&b';') {
        return Err(KeyRefusal::Parameters);
    }
    if rest.iter().any(|&b| b != b' ') {
        return Err(KeyRefusal::Trailing);
    }
    checked(key)
}

/// The key the MCP argument `idempotency_key` carries: the same rule as the
/// header, without the quotes, so `"` and `\` are characters like any other
/// and `ab"c` here is the key `"ab\"c"` is there.
pub fn from_raw(text: &str) -> Result<RawKey, KeyRefusal> {
    if !text.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err(KeyRefusal::NotPrintable);
    }
    checked(text.to_string())
}

/// The length rule, on an unescaped value already known to be printable ASCII
/// (so its length in bytes is its length in characters).
fn checked(key: String) -> Result<RawKey, KeyRefusal> {
    if (MIN_KEY_CHARS..=MAX_KEY_CHARS).contains(&key.len()) {
        Ok(RawKey(key))
    } else {
        Err(KeyRefusal::Length(key.len()))
    }
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
pub fn key_from_headers(headers: &HeaderMap) -> Result<Option<RawKey>, ApiError> {
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
pub fn fingerprint(method: &str, route: &str, path_and_query: &str, body: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(HTTP_TAG);
    for part in [method.as_bytes(), route.as_bytes(), path_and_query.as_bytes(), body] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    hash.finalize().into()
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
pub fn tool_fingerprint(tool: &str, arguments: &Value) -> [u8; 32] {
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
    hash.finalize().into()
}

/// A keyed request, as the edge hands it on: the key, and the fingerprint of
/// the request it came with. The principal and the collection, which complete
/// its scope, are the caller's to add once authorization has named them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotentRequest {
    pub key: RawKey,
    pub fingerprint: [u8; 32],
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
/// same refusals, and the same body. With it, the body is read as bytes through
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

impl<T, S> FromRequest<S> for KeyedJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Some(key) = key_from_headers(req.headers())? else {
            let JsonBody(body) = JsonBody::<T>::from_request(req, state).await?;
            return Ok(Self { body, key: None });
        };
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
/// reads none.
pub struct KeyedNoBody(pub Option<IdempotentRequest>);

impl<S: Send + Sync> FromRequestParts<S> for KeyedNoBody {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(key) = key_from_headers(&parts.headers)? else {
            return Ok(Self(None));
        };
        let fingerprint = fingerprint(
            parts.method.as_str(),
            &route_of(&parts.extensions),
            target_of(&parts.uri),
            b"",
        );
        Ok(Self(Some(IdempotentRequest { key, fingerprint })))
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
    if let Err(closed) = state.idempotency_gate() {
        state.metrics.record_idempotency(KeyedOutcome::Gated);
        return Err(ApiError::bad_request(format!("{MCP_ARGUMENT}: {closed}")));
    }
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

/// The `INFO` line a resumed request writes: a multi-document update or delete
/// that had stopped, sent again with its key and continued past where it
/// stopped. `origin` is the member whose record it resumed from; the counts
/// are the request's so far, across its attempts.
pub fn log_resumed(
    db: &str,
    collection: &str,
    origin: kimmy_core::NodeId,
    matched: u64,
    modified: u64,
    skipped: u64,
) {
    info!(
        db,
        collection,
        origin = %origin,
        matched,
        modified,
        skipped,
        "resumed a keyed write that had stopped"
    );
}

/// The `DEBUG` line a replay writes: `origin` is the member that wrote the
/// record, and `age_ms` how long ago its last commit was, by the record's own
/// clock.
pub fn log_replayed(db: &str, collection: &str, origin: kimmy_core::NodeId, age_ms: u64) {
    debug!(db, collection, origin = %origin, age_ms, "replayed a keyed write");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str) -> Result<String, KeyRefusal> {
        parse_header_value(text.as_bytes()).map(RawKey::into_string)
    }

    const UUID: &str = "6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90";

    // The header's specification. Accepted: a quoted UUID; the bounds, 16 and
    // 255, counted on the unescaped value; the two escapes; a space inside;
    // spaces around the Item. Refused: an empty value, a bare token, an
    // unclosed string, any other escape, a character outside printable ASCII
    // (a non-ASCII byte, a control character, a tab), 15 and 256 characters,
    // parameters, and anything else after the string, a list included.

    #[test]
    fn a_quoted_key_is_its_unquoted_value() {
        assert_eq!(key(&format!("\"{UUID}\"")), Ok(UUID.to_string()));
        assert_eq!(key("\"key with a space in it\""), Ok("key with a space in it".to_string()));
        assert_eq!(key(&format!("  \"{UUID}\"  ")), Ok(UUID.to_string()), "SP around the Item");
    }

    #[test]
    fn the_length_is_the_unescaped_values_and_its_bounds_are_16_and_255() {
        let of = |n: usize| format!("\"{}\"", "k".repeat(n));
        assert_eq!(key(&of(15)), Err(KeyRefusal::Length(15)));
        assert_eq!(key(&of(16)).map(|k| k.len()), Ok(16));
        assert_eq!(key(&of(255)).map(|k| k.len()), Ok(255));
        assert_eq!(key(&of(256)), Err(KeyRefusal::Length(256)));
        assert_eq!(key("\"\""), Err(KeyRefusal::Length(0)));

        // 14 plain characters and two escapes are 16 once unescaped, and 18 as
        // sent: the unescaped count is the one that decides.
        let escaped = format!("\"{}\\\"\\\\\"", "k".repeat(14));
        assert_eq!(escaped.len() - 2, 18);
        assert_eq!(key(&escaped), Ok(format!("{}\"\\", "k".repeat(14))));
        // 254 plain and one escape: 255 unescaped, 256 as sent.
        let at_the_top = format!("\"{}\\\\\"", "k".repeat(254));
        assert_eq!(key(&at_the_top).map(|k| k.len()), Ok(255));
        let over = format!("\"{}\\\\\"", "k".repeat(255));
        assert_eq!(key(&over), Err(KeyRefusal::Length(256)));
    }

    #[test]
    fn only_a_quote_and_a_backslash_are_escaped() {
        assert_eq!(key(&format!("\"{UUID}\\n\"")), Err(KeyRefusal::BadEscape));
        assert_eq!(key(&format!("\"{UUID}\\")), Err(KeyRefusal::Unterminated));
    }

    #[test]
    fn a_value_that_is_not_one_quoted_string_is_refused() {
        assert_eq!(key(""), Err(KeyRefusal::Empty));
        assert_eq!(key("   "), Err(KeyRefusal::Empty));
        assert_eq!(key(UUID), Err(KeyRefusal::NotAString), "a bare token is not a String");
        assert_eq!(key("12345678901234567"), Err(KeyRefusal::NotAString));
        assert_eq!(key(&format!("\"{UUID}")), Err(KeyRefusal::Unterminated));
        assert_eq!(key(&format!("\"{UUID}\";a=1")), Err(KeyRefusal::Parameters));
        assert_eq!(key(&format!("\"{UUID}\";a")), Err(KeyRefusal::Parameters));
        assert_eq!(key(&format!("\"{UUID}\", \"{UUID}\"")), Err(KeyRefusal::Trailing));
        assert_eq!(key(&format!("\"{UUID}\"x")), Err(KeyRefusal::Trailing));
        assert_eq!(key(&format!("\"{UUID}\" ;a=1")), Err(KeyRefusal::Trailing));
    }

    #[test]
    fn a_character_outside_printable_ascii_is_refused() {
        for bad in ["é", "\u{7f}", "\t", "\u{1}"] {
            assert_eq!(key(&format!("\"{UUID}{bad}\"")), Err(KeyRefusal::NotPrintable), "{bad:?}");
        }
        // An obs-text byte, which `HeaderValue` admits and a `&str` cannot hold.
        let mut raw = format!("\"{UUID}").into_bytes();
        raw.extend_from_slice(&[0xff, b'"']);
        assert_eq!(parse_header_value(&raw), Err(KeyRefusal::NotPrintable));
    }

    #[test]
    fn the_mcp_argument_is_the_same_rule_without_quotes() {
        assert_eq!(from_raw(UUID).map(RawKey::into_string), Ok(UUID.to_string()));
        // The characters a header must escape are plain here, and name the same key.
        let raw = format!("{}\"\\", "k".repeat(14));
        assert_eq!(
            from_raw(&raw),
            parse_header_value(format!("\"{}\\\"\\\\\"", "k".repeat(14)).as_bytes())
        );
        assert_eq!(from_raw(&"k".repeat(15)), Err(KeyRefusal::Length(15)));
        assert_eq!(from_raw(&"k".repeat(256)), Err(KeyRefusal::Length(256)));
        assert_eq!(from_raw(""), Err(KeyRefusal::Length(0)));
        assert_eq!(from_raw(&format!("{UUID}é")), Err(KeyRefusal::NotPrintable));
        assert_eq!(from_raw(&format!("{UUID}\n")), Err(KeyRefusal::NotPrintable));
    }

    #[test]
    fn the_header_is_read_from_one_field_line_and_refused_by_name() {
        let mut headers = HeaderMap::new();
        assert_eq!(key_from_headers(&headers).unwrap(), None);
        headers.append(IDEMPOTENCY_KEY, HeaderValue::from_str(&format!("\"{UUID}\"")).unwrap());
        assert_eq!(key_from_headers(&headers).unwrap().map(RawKey::into_string), Some(UUID.into()));
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
            KeyRefusal::Empty,
            KeyRefusal::NotAString,
            KeyRefusal::Unterminated,
            KeyRefusal::BadEscape,
            KeyRefusal::NotPrintable,
            KeyRefusal::Length(0),
            KeyRefusal::Length(300),
            KeyRefusal::Parameters,
            KeyRefusal::Trailing,
        ] {
            let header = header_refused(refusal).message;
            assert!(header.contains(&refusal.to_string()), "{header}");
            let argument = argument_refused(refusal).message;
            assert!(argument.starts_with("idempotency_key: "), "{argument}");
        }
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

    /// What the two log helpers write, as a JSON subscriber formats it: the
    /// level, the message and every field, at its value.
    fn logged(emit: impl FnOnce()) -> Vec<Value> {
        #[derive(Clone, Default)]
        struct Captured(std::sync::Arc<parking_lot::Mutex<Vec<u8>>>);
        impl std::io::Write for Captured {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
            type Writer = Captured;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::TRACE)
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        let out = String::from_utf8(sink.0.lock().clone()).unwrap();
        out.lines().map(|line| serde_json::from_str(line).unwrap()).collect()
    }

    #[test]
    fn a_resume_logs_at_info_with_its_counts_and_a_replay_at_debug() {
        let origin = kimmy_core::NodeId::from_bytes([7; 16]);
        let lines = logged(|| log_resumed("shop", "orders", origin, 1800, 1700, 3));
        assert_eq!(lines.len(), 1, "{lines:?}");
        let line = &lines[0];
        assert_eq!(line["level"], "INFO");
        let fields = &line["fields"];
        assert_eq!(fields["message"], "resumed a keyed write that had stopped");
        assert_eq!(fields["db"], "shop");
        assert_eq!(fields["collection"], "orders");
        assert_eq!(fields["origin"], origin.to_string());
        assert_eq!(
            (fields["matched"].clone(), fields["modified"].clone()),
            (1800.into(), 1700.into())
        );
        assert_eq!(fields["skipped"], 3);

        let lines = logged(|| log_replayed("shop", "orders", origin, 250));
        assert_eq!(lines.len(), 1, "{lines:?}");
        let line = &lines[0];
        assert_eq!(line["level"], "DEBUG", "a replay is the feature working: below the default");
        let fields = &line["fields"];
        assert_eq!(fields["message"], "replayed a keyed write");
        assert_eq!(fields["db"], "shop");
        assert_eq!(fields["collection"], "orders");
        assert_eq!(fields["origin"], origin.to_string());
        assert_eq!(fields["age_ms"], 250);
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
