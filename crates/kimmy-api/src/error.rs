//! HTTP error mapping.
//!
//! Every failure becomes a JSON body with a stable `error` code, so clients can
//! branch on something other than prose.
//!
//! # The code set is closed, and the compiler is what closes it
//!
//! [`ErrorCode`] is an enum rather than a `&'static str`, so a new failure
//! cannot invent a code at a call site. No count is given here, deliberately:
//! this sentence named one, went two codes out of date, and nothing failed —
//! and the claim it makes does not need a number to be true. The wire string,
//! the retry class and the log level all come from exhaustive matches on it:
//! adding a variant does not compile until all three are answered, which is
//! the point — the last two are decisions about client behaviour and about
//! what wakes an operator, and both would otherwise be made by accident.
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
use std::fmt;
use tracing::{Level, error, info, warn};

/// The `Retry-After` a `collection_purging` refusal carries, in seconds.
pub const COLLECTION_PURGING_RETRY_AFTER_SECS: u64 = 5;

/// Every code the API can return, and nothing else.
///
/// **Adding a variant here also means editing `kimmy-client`.** That crate
/// depends on no `kimmy-*` crate by design — it has to see what the Python and
/// Go clients see — so its own `ErrorCode`, its `parse`, its `Display` and the
/// code list in its round-trip test are hand-copied from this one and nothing
/// ties them together. The tests in this workspace fail on a code the *server*
/// documents and does not serve, or serves and does not document, so a new
/// variant is caught here and prompts its author; nothing points that author at
/// `crates/kimmy-client/src/error.rs`, which is what this comment is for. A
/// client meeting an unknown code is not broken — it reads the `retry` class
/// from the envelope, which is exactly why that field exists (ADR-057), and it
/// keeps the string — but the code reaches it as `ErrorCode::Unknown` rather
/// than as a named variant, and a named variant is what a caller matches on.
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
    /// The request ran past `server.request_timeout_secs` and this node gave
    /// up on it (ADR-099).
    Timeout,
    /// A collection cannot be created under this name yet: what a drop of an
    /// earlier collection of the name held is still being removed (ADR-189).
    CollectionPurging,
    /// A write reached the storage engine's durability step and then failed:
    /// it may or may not have been applied, and if it was it replicates.
    OutcomeUnknown,
    /// A request that commits in more than one transaction failed after its
    /// first commit: part of it landed, and the answer says how much
    /// (ADR-192).
    PartiallyApplied,
}

/// What a client may do about a failure.
///
/// **The set is open.** A class this build does not name may be added, and a
/// client treats a class it does not know as `no`: the safe direction, since a
/// client that does not understand the advice does not act on it (ADR-057).
///
/// Several-valued rather than a boolean because KimmyDB is leaderless. Every
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
    /// Read the target back before deciding to send the request again: it may
    /// already have been applied. Only an idempotent request is safe to
    /// resend without reading first.
    Verify,
}

impl Retry {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::No => "no",
            Self::Wait => "wait",
            Self::Elsewhere => "elsewhere",
            Self::Verify => "verify",
        }
    }
}

/// The levels a failure may be logged at, and there are only three.
///
/// Deliberately not [`tracing::Level`], which also has `DEBUG` and `TRACE`.
/// ADR-136 decided that a failure meant to be quieter than `INFO` is not a log
/// event at all — it answers `None` from [`ErrorCode::log_level`] — rather than
/// an event at a level the subscriber happens to filter out, because "do not
/// log this" is a property of the code and not of how the process was started.
/// That rule used to be held by an assertion in `into_response` and a test over
/// `ErrorCode::ALL`; it is held by this type instead, which is why neither is
/// needed to state it any more (ADR-137).
///
/// The consequence worth naming: `Option<LogLevel>` makes the whole vocabulary
/// structural. `None` is *not an event*, and the three variants are the only
/// severities that exist, so `into_response`'s match is exhaustive over three
/// arms with no fallback to get wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    /// Page someone: this node's own state, and the operator's to fix.
    Error,
    /// A rise in these is worth looking at; no single one demands action.
    Warn,
    /// This happened and it is not a fault.
    Info,
}

impl LogLevel {
    /// The `tracing` level this maps to. The one place the two vocabularies
    /// meet, so a caller cannot pick a `tracing::Level` this type cannot say.
    pub fn tracing(self) -> Level {
        match self {
            Self::Error => Level::ERROR,
            Self::Warn => Level::WARN,
            Self::Info => Level::INFO,
        }
    }
}

impl fmt::Display for LogLevel {
    /// The same rendering `tracing::Level` gives, because `docs/operations.md`
    /// publishes these words and `tests/docs.rs` compares the document to
    /// this. A divergence here would be a documentation drift nothing catches.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.tracing(), f)
    }
}

impl ErrorCode {
    /// Every variant, for the tests that hold the specification to this set.
    pub const ALL: [ErrorCode; 22] = [
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
        Self::Timeout,
        Self::CollectionPurging,
        Self::OutcomeUnknown,
        Self::PartiallyApplied,
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
            Self::Timeout => "timeout",
            Self::CollectionPurging => "collection_purging",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::PartiallyApplied => "partially_applied",
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
            // `wait`, not `elsewhere`, deliberately. The deadline is only ever
            // reached while the request is *waiting* — for the rest of its
            // body, or for an upstream provider — and neither improves by
            // moving: a slow upload is slow to every node, and every node
            // calls the same provider. `elsewhere` would send the same slow
            // upload round the whole cluster (ADR-099).
            Self::Timeout => Retry::Wait,
            // The removal ends by itself on this node and the creation then
            // succeeds here; another member may still be removing its own copy
            // of the same drop, so moving does not help.
            Self::CollectionPurging => Retry::Wait,
            // It may already have happened, and replicates if it did: read
            // before resending, which neither waiting nor moving replaces.
            Self::OutcomeUnknown => Retry::Verify,
            // Part of it is there and replicates. Resending all of it
            // re-applies that part; which part is not in the answer, so the
            // client reads back, or resends a request built to skip what is
            // done (ADR-192).
            Self::PartiallyApplied => Retry::Verify,

            // Local to this node, and replication means a peer can answer.
            // A storage failure here says nothing about the peer's disk, and
            // a missing API key or a bad snapshot is this node's own state.
            Self::Internal | Self::Misconfigured | Self::Snapshot => Retry::Elsewhere,
        }
    }

    /// How loudly this node talks to its operator about a failure it answered
    /// with, or `None` for one it does not log at all.
    ///
    /// **The discriminator is actionability, not HTTP class**: is the fix in
    /// the operator's hands, or the caller's? That question cuts across the
    /// 5xx set — a reserved capability is refused with a 501 that no operator
    /// can do anything about — which is why the status could never have
    /// scoped this (ADR-136).
    ///
    /// The level a code takes here is a claim about what an alert built on it
    /// would mean, so read each arm as one: `ERROR` says *page someone*,
    /// `WARN` says *a rise in these is worth looking at*, `INFO` says *this
    /// happened and it is not a fault*, and `None` says *this is not an event
    /// at all*.
    pub fn log_level(self) -> Option<LogLevel> {
        match self {
            // Not logged, at any level. These are the caller's to fix and the
            // caller already holds the answer, in a response naming exactly
            // what was wrong. Logging them would be an access log of nothing
            // but the failures — half a record, and one this server has never
            // kept. `kimmy_responses_total{class="4xx"}` counts them, the
            // audit log records the authorization decisions among them, and
            // neither costs a line per bad request.
            //
            // `None` rather than a level the subscriber filters out, so that
            // an operator raising `RUST_LOG` to debug something else does not
            // suddenly acquire that half access log. "Do not log this" is a
            // property of the code, not of how the process was started.
            Self::BadRequest
            | Self::PayloadTooLarge
            | Self::UnsupportedMediaType
            | Self::Unauthorized
            | Self::Forbidden
            | Self::NotFound
            | Self::Conflict
            | Self::DuplicateKey
            | Self::UniqueViolation
            | Self::NoVectors
            | Self::ResumeTokenExpired
            | Self::RateLimited
            | Self::Stale
            // Expected, bounded and explained by the response itself; the
            // drop purger's own log lines are the operator's record.
            | Self::CollectionPurging => None,

            // Reserved and unbuilt, by default. The reference says this will
            // be refused, the caller asked for it anyway, and there is no
            // operator action — the capability exists on no node and no
            // configuration turns it on. `INFO` rather than `None` because
            // unlike a 4xx it is the *server* declining, and an operator
            // sizing up what callers are reaching for should be able to see
            // it without turning on a firehose.
            //
            // One source overrides this upward. A node that cannot serve
            // embeddings the rest of the cluster expects returns the same
            // code for an entirely operator-owned condition, and raises
            // itself to `ERROR` at construction — see
            // [`ApiError::level_override`] and `vectors.rs`. Lowering the
            // whole code to suit its commoner source would have hidden that
            // one, which is the failure this level split exists to prevent.
            Self::NotImplemented => Some(LogLevel::Info),

            // An upstream embedding provider failed, and the comment on
            // `retry()` above says whose fault that is: the upstream's. No
            // single occurrence demands an operator do anything — a provider
            // drops a connection and the client retries — but a *rise* is a
            // quota, a revoked key, or a provider that is down, and those are
            // all the operator's. `WARN` is the level that says exactly that.
            Self::ProviderError => Some(LogLevel::Warn),

            // `WARN`, uniformly, and deliberately not split by cause. The
            // deadline is only ever reached while the request is *waiting* —
            // for the rest of its body, or for an upstream provider — and
            // those have different owners: a slow client is the caller's, a
            // slow provider is the operator's. But the deadline is enforced
            // by a middleware layer wrapping the whole handler
            // (`limits::enforce_timeout`), which learns only that the future
            // did not finish; the cause is somewhere inside a future that no
            // longer exists. Threading it out would mean every awaiting site
            // reporting what it was waiting on, which is a large change to
            // pay for a log level. `WARN` is the honest answer for both: a
            // rise in abandoned requests is operationally interesting even
            // when each one is a slow client, and `WARN` keeps it visible
            // without paging (ADR-099, ADR-136).
            Self::Timeout => Some(LogLevel::Warn),

            // A genuine fault in this node: storage failed, or something that
            // cannot happen did. Nothing a caller sends causes it and nothing
            // a caller changes fixes it.
            Self::Internal => Some(LogLevel::Error),
            // The storage failed at or after a write's durability step: the
            // same fault as `internal`, and the operator's.
            Self::OutcomeUnknown => Some(LogLevel::Error),
            // By default the operator's: the storage failed, or the node
            // stopped, part way through. A cause that is the caller's, such
            // as an operator a later document cannot take, lowers it to
            // `WARN` where the error is made.
            Self::PartiallyApplied => Some(LogLevel::Error),

            // An operator must set something. This node cannot build the
            // provider a replicated vector configuration names — an unset
            // environment variable, an egress policy that refuses it, a
            // profile it does not define — while some other member could,
            // which makes it a member configured unlike its cluster. Correctly
            // loud: it is silent until a caller happens to search that
            // collection on this node, so the first occurrence is the whole
            // warning an operator gets.
            Self::Misconfigured => Some(LogLevel::Error),

            // A vector index snapshot on this node's disk could not be written
            // or could not be read back: an I/O error under the snapshot
            // directory, or a snapshot file whose metadata does not parse. All
            // of that is this node's own storage, and no request changes it.
            // The cache is supposed to absorb it — a snapshot that will not
            // load is discarded and the graph rebuilt — so one reaching a
            // response means that absorption did not happen, which is a fault
            // in this node on top of whatever the disk did. Both halves are
            // the operator's.
            Self::Snapshot => Some(LogLevel::Error),
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
    /// A log level that differs from the code's default, set where the source
    /// of the failure is known and the default is wrong for it.
    ///
    /// This exists because a per-code level alone cannot split
    /// `not_implemented`: its two sources *share the code*, and one of them —
    /// a node that cannot build local embeddings — is an operator-owned
    /// condition wearing the same wire code as a caller asking for a reserved
    /// feature. Both sources are explicit constructions, so the level is
    /// decided where the cause is still in hand and nothing is threaded
    /// through a call chain to reach it (ADR-136).
    ///
    /// `Some(level)` raises or lowers; there is no way to say "and do not log
    /// this one", because no site has wanted to suppress an occurrence of a
    /// code that is otherwise logged, and a per-instance silence is the kind
    /// of thing that hides a fault rather than a nuisance.
    pub level_override: Option<LogLevel>,
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
    /// Fields the envelope carries beside `error`, `message` and `retry`,
    /// for the one code whose answer is more than a code: `partially_applied`,
    /// which says what landed (`applied`) and why the rest did not (`cause`).
    ///
    /// Siblings at the top level, never a nested object: `error` stays a
    /// string, which is what every client already parses (ADR-057, ADR-192).
    ///
    /// Boxed, and absent on every other code, so an `ApiError` stays small
    /// on the many paths that return one.
    pub extra: Option<Box<serde_json::Map<String, serde_json::Value>>>,
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
            level_override: None,
            challenge_description: None,
            extra: None,
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

    /// The same error logged at a level other than its code's default.
    ///
    /// Takes a [`LogLevel`] and not a [`tracing::Level`], so "quieter than
    /// `INFO`" is not a thing a caller can ask for. It used to be: the
    /// parameter accepted any `tracing::Level` while `into_response` handled
    /// three, so `at_level(Level::DEBUG)` tripped a `debug_assert!` in a debug
    /// build and logged at `INFO` in a release one (ADR-137).
    pub fn at_level(mut self, level: LogLevel) -> Self {
        self.level_override = Some(level);
        self
    }

    /// The retry hint a client will see.
    pub fn retry(&self) -> Retry {
        self.retry_override.unwrap_or_else(|| self.code.retry())
    }

    /// The level this failure is logged at, or `None` for one that is not
    /// logged. The instance's own answer wins over its code's default.
    pub fn log_level(&self) -> Option<LogLevel> {
        self.level_override.or_else(|| self.code.log_level())
    }

    /// A creation refused while a drop of the same name is still being purged
    /// (ADR-189). `Retry-After` is a hint: the purge takes as long as the
    /// dropped collection was large, and each retry is two seeks.
    pub fn collection_purging(db: &str, name: &str) -> Self {
        Self {
            retry_after_secs: Some(COLLECTION_PURGING_RETRY_AFTER_SECS),
            ..Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::CollectionPurging,
                format!(
                    "{db}.{name} was dropped and what it held is still being removed; it can be \
                     created again once that finishes"
                ),
            )
        }
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

    /// The request outlived the server's deadline for it (ADR-099).
    ///
    /// 503 rather than 408 or 504. RFC 9110 §15.5.9 makes 408 a statement
    /// about an idle connection — "the server did not receive a complete
    /// request message within the time that it was prepared to wait" — and
    /// tells a client it may simply repeat the request, which browsers and
    /// several HTTP libraries do silently; that is the wrong instruction for a
    /// request this node may have partly acted on. 504 is a gateway's answer
    /// about an upstream, and this node is the origin. 503 says what is true:
    /// this server did not handle this request, and the envelope's `retry`
    /// says what to do about it. No `Retry-After`, because the server has no
    /// idea when a slower client or a slower provider will be faster.
    pub fn timeout(after: std::time::Duration) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Timeout,
            format!(
                "the request was not completed within {} seconds (server.request_timeout_secs) \
                 and was abandoned",
                after.as_secs()
            ),
        )
    }

    /// A write that waited its whole budget for the storage writer (ADR-151).
    ///
    /// The same code and status as [`Self::timeout`]: to the client it is
    /// the same fact — the request did not complete inside
    /// `server.request_timeout_secs` and nothing was done — and the retry
    /// hint is the same. The message says what the time went on.
    pub fn writer_busy(waited: std::time::Duration) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Timeout,
            format!(
                "the write waited {} seconds (server.request_timeout_secs) for the storage \
                 writer, which another transaction held throughout, and was abandoned; \
                 nothing was written",
                waited.as_secs()
            ),
        )
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

    /// A write whose outcome is unknown: see [`ErrorCode::OutcomeUnknown`].
    pub fn outcome_unknown() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::OutcomeUnknown,
            "the write reached the storage engine's durability step and then failed, so it may \
             or may not have been applied; if it was, it replicates. Read it back before sending \
             it again, unless the write is idempotent",
        )
    }

    /// Part of a request that commits in more than one transaction landed,
    /// and then it failed (ADR-192).
    ///
    /// `cause` is answered as the code and message it would have been on its
    /// own, which are already reduced to what is safe to return; its storage
    /// detail is logged by that mapping, never returned. A node stopping has
    /// no code of its own: `stopping` names the drain deadline, and
    /// `storage_failed` the storage failure that stops the process (ADR-188).
    pub fn partially_applied(applied: &kimmy_storage::Applied, cause: StorageError) -> Self {
        // Who the cause belongs to decides the level: a stop at the drain
        // deadline is a shutdown doing its job (`WARN`), a caller's refusal
        // is the caller's (`WARN`), and everything else is this node's
        // (`ERROR`), a storage failure included.
        let (cause_code, cause_message, level) = match cause {
            StorageError::Stopping(reason @ kimmy_storage::StopReason::DrainDeadline) => {
                ("stopping", reason.to_string(), LogLevel::Warn)
            }
            StorageError::Stopping(reason @ kimmy_storage::StopReason::StorageFailed) => {
                ("storage_failed", reason.to_string(), LogLevel::Error)
            }
            other => {
                let mapped = ApiError::from(other);
                let level =
                    if mapped.status.is_server_error() { LogLevel::Error } else { LogLevel::Warn };
                (mapped.code.as_str(), mapped.message, level)
            }
        };
        let (what, applied) = match applied {
            kimmy_storage::Applied::Modify { matched, modified, commits, in_doubt } => (
                format!(
                    "{matched} matching documents in {commits} commits were written, and \
                     {in_doubt} more may have been"
                ),
                json!({
                    "matched": matched,
                    "modified": modified,
                    "commits": commits,
                    "in_doubt": in_doubt,
                }),
            ),
            kimmy_storage::Applied::DropDatabase { dropped, in_doubt } => (
                format!(
                    "{} collections were dropped{}",
                    dropped.len(),
                    in_doubt
                        .as_ref()
                        .map(|c| format!(", and {c} may have been"))
                        .unwrap_or_default()
                ),
                json!({ "dropped": dropped, "in_doubt": in_doubt }),
            ),
        };
        let mut e = Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::PartiallyApplied,
            format!(
                "the request was partly applied and then stopped ({cause_code}: {cause_message}): \
                 {what}. What was written stands and replicates; read the target back before \
                 sending the request again"
            ),
        );
        let mut extra = serde_json::Map::new();
        extra.insert("applied".into(), applied);
        extra.insert("cause".into(), json!({ "code": cause_code, "message": cause_message }));
        e.extra = Some(Box::new(extra));
        e.level_override = Some(level);
        e
    }

    /// A write this node did not begin, because it is stopping (ADR-192).
    /// Nothing was written. `503`, and `elsewhere`: the node is going away,
    /// and another member serves. `WARN` for the shutdown doing its job;
    /// the storage failure behind the other reason is already an `ERROR`.
    pub fn node_stopping(reason: kimmy_storage::StopReason) -> Self {
        let mut e = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal,
            format!(
                "this node did not begin the write because it is shutting down ({reason}); \
                 nothing was written. Send it to another member"
            ),
        );
        e.level_override = Some(LogLevel::Warn);
        e
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // The gate is the code's level, not the status class. `is_server_error`
        // used to stand in for "the operator needs to know", and it is not that
        // — a 501 for a reserved capability is a documented refusal a caller
        // asked for, and it wrote an ERROR line on whichever member answered
        // it. The level a code carries is the property that was actually
        // wanted, and the one instance where the code is not enough to decide
        // overrides it at construction (ADR-136).
        //
        // The three arms are written out because a `tracing` macro takes a
        // constant level; there is no `event!(level, …)` that accepts a value.
        // They are also all of them: `LogLevel` has three variants, so this
        // match is exhaustive and carries no fallback. It used to carry one,
        // for a `tracing::Level` below INFO that `at_level` accepted and
        // nothing here could log — `debug_assert!(false)` and then `info!`,
        // which panics a debug build and quietly logs one level too loud in a
        // release one. Narrowing the type deleted the state rather than the
        // guard (ADR-137).
        //
        // The event name is the *same* on all three, deliberately. Varying
        // it would put a second discriminator beside the level — one nothing
        // publishes and no test pins — and an operator grepping for one
        // wording would silently miss the lines written under the other. That
        // is the shape of trap this whole change removes, so severity is the
        // level's job alone, and `code` is what says which failure it was.
        //
        // The name rides in an `event` field and there is no format string.
        // A `tracing` macro's format string *is* a field, named `message`, so
        // `error!(code, message, "{EVENT}")` wrote two fields of that name —
        // and the JSON layer `kimmyd` runs serialises fields in order without
        // deduplicating, so every line carried `"message"` twice and a parser
        // kept whichever one it kept. `operations.md` promises the
        // client-facing text is in `message`; that is only true when nothing
        // else is (ADR-144).
        const EVENT: &str = "request failed";
        if let Some(level) = self.log_level() {
            let (code, message) = (self.code.as_str(), self.message.as_str());
            match level {
                LogLevel::Error => error!(event = EVENT, code, message),
                LogLevel::Warn => warn!(event = EVENT, code, message),
                LogLevel::Info => info!(event = EVENT, code, message),
            }
        }
        // `retry` rides in the envelope rather than living only in the
        // specification, so a client meeting a code added after it was written
        // still knows what to do with it. That is what makes a new code an
        // additive change rather than one that needs every client updated.
        let mut body = json!({
            "error": self.code.as_str(),
            "message": self.message,
            "retry": self.retry().as_str(),
        });
        if let (serde_json::Value::Object(fields), Some(extra)) = (&mut body, self.extra) {
            for (key, value) in *extra {
                fields.entry(key).or_insert(value);
            }
        }
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

/// A query string the route cannot read: a parameter it does not define, or
/// one whose value is not the type it takes.
///
/// Axum's text is kept — it names the parameter, which is the whole point of
/// refusing (ADR-121) — and only the envelope is added. Always `400`: unlike a
/// body, a query string has no "not JSON" and "not this JSON" to tell apart.
impl From<axum::extract::rejection::QueryRejection> for ApiError {
    fn from(rejection: axum::extract::rejection::QueryRejection) -> Self {
        Self::bad_request(rejection.body_text())
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
            // The writer stayed held for the whole of the request's budget
            // (ADR-151). Nothing was written: only a request that has not
            // yet committed anything waits within a budget, and one that
            // has waits without one (ADR-192). So the documented `timeout`
            // refusal, whose retry hint is to wait, is the honest answer.
            StorageError::WriterBusy { waited } => ApiError::writer_busy(waited),
            // A creation over a dropped life's rows, which the drop purger is
            // removing and has been asked to take next (ADR-189). Nothing was
            // written; retrying after a few seconds succeeds once it is done.
            StorageError::CollectionPurging { db, name, .. } => {
                ApiError::collection_purging(&db, &name)
            }
            // Its durability step began and failed: the write may have
            // happened, and if it did it replicates. Never "it failed". The
            // cause names on-disk internals, so it is logged, not returned.
            StorageError::OutcomeUnknown(cause) => {
                error!(error = %cause, "a write failed at its durability step; its outcome is unknown");
                ApiError::outcome_unknown()
            }
            StorageError::PartiallyApplied { applied, cause } => {
                ApiError::partially_applied(&applied, *cause)
            }
            // A write refused because the node has closed its storage at the
            // end of a shutdown (ADR-192), or a later transaction refused on
            // its own. Nothing was written, as with a busy writer, and this
            // node is going away: another member serves.
            StorageError::Stopping(reason) => ApiError::node_stopping(reason),
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
            AuthError::UserExists(_)
            | AuthError::RoleExists(_)
            | AuthError::LastUser
            | AuthError::LastEnabledUser => ApiError::conflict(e.to_string()),
            // Both are configuration refusals raised before the server ever
            // serves, so neither can reach a request. Mapped rather than
            // matched loosely so that adding a variant stays a compile error
            // here instead of silently becoming a 400.
            AuthError::WeakSecret { .. }
            | AuthError::PreviousSecretIsCurrent
            | AuthError::AdminNotFederatable { .. }
            | AuthError::EmptyRoleMapping { .. }
            | AuthError::InvalidResourceIdentifier { .. } => ApiError::bad_request(e.to_string()),
            // The storage error's own answer: `outcome_unknown` for a
            // commit that may have happened, and every other code as for a
            // document write.
            AuthError::Storage(e) => ApiError::from(e),
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

    /// A writer a test can read back, so an assertion can be made about the
    /// line a failure produced rather than about the call that produced it.
    /// The same shape `audit.rs` uses, and for the same reason: the only way
    /// to check what a subscriber sees is to capture what one formats.
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

    /// Whatever this error writes to the log on its way to becoming a
    /// response. Empty for one that is not logged.
    fn logged(error: ApiError) -> String {
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(Level::TRACE)
            .without_time()
            .with_ansi(false)
            .finish();
        // Scoped to this thread, so a parallel test in this binary does not
        // collect the line and read it as its own.
        tracing::subscriber::with_default(subscriber, || {
            let _ = error.into_response();
        });
        let out = sink.0.lock().clone();
        String::from_utf8(out).expect("utf-8")
    }

    #[test]
    fn every_code_logs_at_the_level_its_actionability_earns() {
        // Written out rather than derived, so that changing a level is a
        // change to this table and not a side effect of editing a match arm.
        // Each level is a claim about what an alert on it would mean, and
        // ADR-136 argues them one at a time; this is that argument's fixture.
        use ErrorCode::*;
        let expected: [(ErrorCode, Option<LogLevel>); 22] = [
            // The caller's, every one, and answered in full by the response.
            (BadRequest, None),
            (PayloadTooLarge, None),
            (UnsupportedMediaType, None),
            (Unauthorized, None),
            (Forbidden, None),
            (NotFound, None),
            (Conflict, None),
            (DuplicateKey, None),
            (UniqueViolation, None),
            (NoVectors, None),
            (ResumeTokenExpired, None),
            (RateLimited, None),
            (Stale, None),
            (CollectionPurging, None),
            // Refused on purpose, with no operator action to take. The
            // default only: `vectors.rs` raises its own source above this.
            (NotImplemented, Some(LogLevel::Info)),
            // Somebody else's fault, or nobody's; a rise is the finding.
            (ProviderError, Some(LogLevel::Warn)),
            (Timeout, Some(LogLevel::Warn)),
            // This node's own state, and the operator's to fix.
            (Internal, Some(LogLevel::Error)),
            (OutcomeUnknown, Some(LogLevel::Error)),
            (PartiallyApplied, Some(LogLevel::Error)),
            (Misconfigured, Some(LogLevel::Error)),
            (Snapshot, Some(LogLevel::Error)),
        ];
        assert_eq!(
            expected.len(),
            ErrorCode::ALL.len(),
            "a new code must be given a level here as well as in the match"
        );
        for (code, level) in expected {
            assert_eq!(code.log_level(), level, "{} logs at the wrong level", code.as_str());
        }
    }

    #[test]
    fn no_code_is_logged_below_info() {
        // Documentation now, not enforcement. `LogLevel` has three variants
        // and `log_level` returns `Option<LogLevel>`, so "a code below INFO"
        // is not a state that can be written down — the rule ADR-136 argued
        // is held by the type, and this asserts what the type already proves
        // (ADR-137). It stays because the rule is worth stating where someone
        // adding a code will read it, and because it is the natural place to
        // pin the one thing still worth pinning: that the three variants map
        // onto the `tracing` levels a reader of `operations.md` expects.
        //
        // The real drift risk moved to `LogLevel::tracing` and `Display`. If
        // `Warn` ever rendered as anything but `WARN`, `tests/docs.rs` would
        // compare the published table against a different word.
        assert_eq!(LogLevel::Error.tracing(), Level::ERROR);
        assert_eq!(LogLevel::Warn.tracing(), Level::WARN);
        assert_eq!(LogLevel::Info.tracing(), Level::INFO);
        assert_eq!(
            [LogLevel::Error.to_string(), LogLevel::Warn.to_string(), LogLevel::Info.to_string()],
            ["ERROR", "WARN", "INFO"],
            "operations.md publishes these words and tests/docs.rs compares against them"
        );

        // `tracing` orders `Level` by verbosity, so "below INFO" is `>` and
        // not `<`. Pinned here rather than assumed, because getting it the
        // wrong way round would leave a check that passes on everything.
        assert!(Level::DEBUG > Level::INFO && Level::ERROR < Level::INFO);
        for code in ErrorCode::ALL {
            if let Some(level) = code.log_level() {
                assert!(
                    level.tracing() <= Level::INFO,
                    "{} maps to {level}, which is below INFO; use `None` to say it is not \
                     logged, or raise it — anything else is emitted at a level the document \
                     does not claim",
                    code.as_str()
                );
            }
        }
    }

    #[test]
    fn a_refusal_the_caller_caused_writes_no_line_at_all() {
        // The property an operator's alert rule rests on: one client sending
        // requests this API documents as refusals cannot make a member look
        // unhealthy. `None` rather than a quiet level, so this holds however
        // the process was started — an operator who raised the filter to
        // debug something else does not acquire a log line per bad request.
        for code in ErrorCode::ALL.into_iter().filter(|c| c.log_level().is_none()) {
            let error = ApiError::new(StatusCode::BAD_REQUEST, code, "whatever the caller sent");
            assert_eq!(logged(error), "", "{} must not be logged", code.as_str());
        }
    }

    #[test]
    fn the_log_gate_is_the_codes_level_and_no_longer_the_status_class() {
        // Before ADR-136 this was `status.is_server_error()`, so all three of
        // these wrote an identical ERROR line. They are three different
        // statements about who has to act, and now they read as three.
        let fault = logged(ApiError::internal("the disk"));
        assert!(fault.contains("ERROR"), "a genuine fault still pages: {fault}");
        assert!(fault.contains(r#"code="internal""#), "{fault}");

        let waited = logged(ApiError::timeout(std::time::Duration::from_secs(30)));
        assert!(waited.contains("WARN"), "an abandoned request is visible, not loud: {waited}");
        assert!(!waited.contains("ERROR"), "{waited}");

        let reserved: ApiError =
            CoreError::Unsupported("coordinated unique enforcement".into()).into();
        let reserved = logged(reserved);
        assert!(reserved.contains("INFO"), "a documented refusal is not a fault: {reserved}");
        assert!(!reserved.contains("ERROR"), "{reserved}");

        // And all three carry the same event name, so one query finds every
        // logged failure and the level is the only thing that separates them.
        // A wording that varied by level would be a second discriminator
        // nothing publishes: an operator grepping for one of them would miss
        // the lines written under the other, silently.
        for line in [&fault, &waited, &reserved] {
            assert!(
                line.contains(r#"event="request failed""#),
                "the event name must not vary: {line}"
            );
        }
    }

    /// The same line, through the layer `kimmyd` actually runs.
    ///
    /// `logged` above uses the plain formatter, which renders every field it
    /// is handed and so cannot show two of one name as anything but two
    /// fields. The JSON layer serialises them in order without deduplicating,
    /// which is where a second `message` became a line a parser silently
    /// halves (ADR-144). This is `logging.rs`'s `LogFormat::Json` layer as
    /// closely as a unit test can build it: `fmt().json()` with the target on,
    /// so the field set and the nesting are the ones an operator's pipeline
    /// sees.
    fn logged_as_json(error: ApiError) -> String {
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_target(true)
            .with_writer(sink.clone())
            .with_max_level(Level::TRACE)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let _ = error.into_response();
        });
        let out = sink.0.lock().clone();
        String::from_utf8(out).expect("utf-8")
    }

    /// Every object key in `raw`, at every depth, in the order written — with
    /// repeats kept, which is the one thing a parser will not do for us.
    ///
    /// A hand-rolled walk rather than `serde_json`, because the defect being
    /// checked for is exactly the one a JSON parser hides: a duplicate key is
    /// legal to emit and every parser keeps one of the two without saying so.
    /// Only enough of the grammar to find keys — strings with escapes, and
    /// the `{`/`}` nesting that says which object a key belongs to.
    fn keys_as_written(raw: &str) -> Vec<(usize, String)> {
        let mut keys = Vec::new();
        let mut depth = 0usize;
        let mut chars = raw.char_indices().peekable();
        while let Some((_, c)) = chars.next() {
            match c {
                '{' => depth += 1,
                '}' => depth -= 1,
                '"' => {
                    let mut text = String::new();
                    let mut escaped = false;
                    for (_, c) in chars.by_ref() {
                        match (escaped, c) {
                            (true, _) => {
                                escaped = false;
                                text.push(c);
                            }
                            (false, '\\') => escaped = true,
                            (false, '"') => break,
                            (false, _) => text.push(c),
                        }
                    }
                    // A string followed by `:` is a key; anything else is a
                    // value and not ours.
                    while matches!(chars.peek(), Some((_, ' '))) {
                        chars.next();
                    }
                    if matches!(chars.peek(), Some((_, ':'))) {
                        keys.push((depth, text));
                    }
                }
                _ => {}
            }
        }
        keys
    }

    #[test]
    fn a_json_key_walk_sees_the_repeat_a_parser_would_swallow() {
        // The walker is the assertion below's only witness, so it is checked
        // against a line whose duplicate is known — the shape the defect had.
        let twice = r#"{"level":"ERROR","fields":{"message":"request failed","code":"internal","message":"the \"disk\": gone"},"target":"t"}"#;
        assert_eq!(
            keys_as_written(twice),
            vec![
                (1, "level".to_string()),
                (1, "fields".to_string()),
                (2, "message".to_string()),
                (2, "code".to_string()),
                (2, "message".to_string()),
                (1, "target".to_string()),
            ],
            "escaped quotes and a colon inside a value do not make keys, and a repeat is kept"
        );
        // And the parser does what the walker exists to get around.
        let parsed: serde_json::Value = serde_json::from_str(twice).unwrap();
        assert_eq!(parsed["fields"].as_object().unwrap().len(), 2, "one of the two is gone");
    }

    #[test]
    fn a_failed_request_line_carries_event_code_and_message_once_each() {
        // The contract `operations.md` publishes: the event name in `event`,
        // the code in `code`, the client-facing text in `message` — three
        // fields, each written once. The macro used to take the event name as
        // its format string, which is a field called `message`, beside the
        // explicit `message` field; the JSON layer wrote both, and whichever
        // one a parser kept, the line lied about the other (ADR-144).
        //
        // All three levels, because they are three macro invocations and a
        // fix to one is not a fix to the others.
        let cases: [(ApiError, &str, &str); 3] = [
            (ApiError::internal("the disk"), "ERROR", "internal"),
            (ApiError::timeout(std::time::Duration::from_secs(30)), "WARN", "timeout"),
            (
                CoreError::Unsupported("coordinated unique enforcement".into()).into(),
                "INFO",
                "not_implemented",
            ),
        ];
        for (error, level, code) in cases {
            // The text the response body carries; the line must carry the
            // same one, whatever the constructor chose to say.
            let message = error.message.clone();
            let raw = logged_as_json(error);
            let lines: Vec<&str> = raw.lines().collect();
            assert_eq!(lines.len(), 1, "one failure, one line: {raw:?}");
            let line = lines[0];

            // (a) What the parsed object says.
            let parsed: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}"));
            assert_eq!(parsed["level"], level, "{line}");
            let fields = parsed["fields"].as_object().unwrap_or_else(|| panic!("{line}"));
            assert_eq!(fields["event"], "request failed", "{line}");
            assert_eq!(fields["code"], code, "{line}");
            assert_eq!(
                fields["message"], message,
                "the client-facing text, not the event name: {line}"
            );
            assert_eq!(
                fields.keys().collect::<Vec<_>>(),
                ["event", "code", "message"],
                "exactly the three fields the document names, in the order they are logged: {line}"
            );

            // (b) What the bytes say — the parsed object cannot show a key
            // that was written twice, so the raw line is walked as well and
            // its key count held to the parser's.
            let written = keys_as_written(line);
            let parsed_count = count_keys(&parsed);
            assert_eq!(
                written.len(),
                parsed_count,
                "a key was written more than once and the parser kept one of them: {line}"
            );
            let in_fields: Vec<&str> =
                written.iter().filter(|(d, _)| *d == 2).map(|(_, k)| k.as_str()).collect();
            assert_eq!(in_fields, ["event", "code", "message"], "{line}");
        }
    }

    /// Object keys at every depth of a parsed value — what [`keys_as_written`]
    /// counts, minus the repeats a parser has already dropped.
    fn count_keys(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::Object(map) => {
                map.len() + map.values().map(count_keys).sum::<usize>()
            }
            serde_json::Value::Array(items) => items.iter().map(count_keys).sum(),
            _ => 0,
        }
    }

    #[test]
    fn an_instance_can_be_louder_than_its_code() {
        // The override exists because `not_implemented`'s two sources share
        // the code and do not share an owner; `vectors.rs` holds the one use
        // of it, and `vectors.rs`'s own tests check that source end to end.
        let raised =
            ApiError::new(StatusCode::NOT_IMPLEMENTED, ErrorCode::NotImplemented, "no model")
                .at_level(LogLevel::Error);
        assert_eq!(raised.log_level(), Some(LogLevel::Error));
        assert_eq!(
            ErrorCode::NotImplemented.log_level(),
            Some(LogLevel::Info),
            "the default is untouched"
        );
        assert!(logged(raised).contains("ERROR"));
    }

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
    fn a_write_whose_outcome_is_unknown_is_never_answered_as_failed() {
        // Not `internal`, which reads as "it failed": the write may have
        // happened, and the client must read before it resends.
        let e: ApiError =
            StorageError::OutcomeUnknown("sync_data: /var/lib/kimmy/kimmy.redb EIO".into()).into();
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(e.code, ErrorCode::OutcomeUnknown);
        assert_eq!(e.code.as_str(), "outcome_unknown");
        assert_eq!(e.code.retry(), Retry::Verify);
        assert_eq!(e.code.retry().as_str(), "verify");
        assert!(!e.message.contains("/var/lib"), "the cause is logged, not returned");
    }

    #[test]
    fn credential_failures_do_not_distinguish_the_cause() {
        let e: ApiError = AuthError::InvalidCredentials.into();
        assert_eq!(e.status, StatusCode::UNAUTHORIZED);
        assert!(!e.message.to_lowercase().contains("user not found"));
    }

    #[test]
    fn every_token_refusal_keeps_the_generic_challenge() {
        // ADR-096 carved out one specific `error_description` for a lifetime
        // refusal. ADR-112 removed the refusal, so nothing on the token path
        // is specific any more: every 401 says the same thing, which is the
        // property the uniform challenge had before ADR-096 and has again.
        for e in [AuthError::TokenExpired, AuthError::InvalidToken] {
            let e: ApiError = e.into();
            assert_eq!(e.status, StatusCode::UNAUTHORIZED);
            assert_eq!(e.code, ErrorCode::Unauthorized);
            assert!(e.challenge_description.is_none(), "{:?}", e.challenge_description);
        }
    }

    #[test]
    fn forbidden_carries_no_detail_about_the_target() {
        // A message naming the collection would let a caller probe for objects
        // they cannot access.
        let e = ApiError::forbidden();
        assert_eq!(e.status, StatusCode::FORBIDDEN);
        assert!(!e.message.contains("collection"));
    }

    #[test]
    fn a_partial_answer_names_why_the_node_stopped_and_logs_by_whose_fault_it_is() {
        let applied =
            kimmy_storage::Applied::Modify { matched: 1, modified: 1, commits: 1, in_doubt: 0 };
        let cause_of = |e: &ApiError| e.extra.as_ref().unwrap()["cause"]["code"].clone();

        // The shutdown doing its job: `stopping`, not a page.
        let drain = ApiError::partially_applied(
            &applied,
            StorageError::Stopping(kimmy_storage::StopReason::DrainDeadline),
        );
        assert_eq!(cause_of(&drain), "stopping");
        assert_eq!(drain.log_level(), Some(LogLevel::Warn));

        // The storage failing is not the shutdown deadline, and is a page.
        let failed = ApiError::partially_applied(
            &applied,
            StorageError::Stopping(kimmy_storage::StopReason::StorageFailed),
        );
        assert_eq!(cause_of(&failed), "storage_failed");
        assert_eq!(failed.log_level(), Some(LogLevel::Error));
        assert!(!failed.message.contains("shutdown deadline"), "{}", failed.message);
    }
}
