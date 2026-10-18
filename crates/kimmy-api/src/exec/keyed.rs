//! A keyed write in the executor (B5): what the engine is handed, and what a
//! fresh answer, a replay and a refusal count.
//!
//! The write functions in `exec` take the edge's [`IdempotentRequest`] and,
//! once `authorize` has named the principal and the collection, build a
//! [`KeyedCall`]: the principal's scope hashed with the key, and the request's
//! fingerprint. The engine's keyed twin answers fresh, with what the unkeyed
//! call answers, or with the first attempt's record; both are rendered by the
//! one function of `crate::outcome` that renders the route's answer, so a
//! replay is the first answer byte for byte, marked `Idempotency-Replayed`.
//!
//! The two log lines a replay and a resume write are the engine's: it finds the
//! record, so it names the record's origin. What is counted here is
//! `kimmy_idempotency_requests_total` by outcome, for every keyed request this
//! member answered with a write's answer or one of the key's own refusals, and
//! `kimmy_idempotency_peer_replays_total`.

use kimmy_storage::{IdempotencyRecord, KeyedWrite, StorageError};
use serde_json::Value;

use crate::error::ApiError;
use crate::idempotency::{IdempotentRequest, KeyedOutcome, WriteAnswer};
use crate::state::{Auth, SharedState};

/// One keyed write: the engine's handle on it, and the state its answer is
/// counted against.
pub(super) struct KeyedCall<'a> {
    state: &'a SharedState,
    /// What the engine keys the write by.
    pub(super) write: KeyedWrite,
}

impl<'a> KeyedCall<'a> {
    /// The call for `request`, made by `auth`'s principal. The scope is the
    /// principal's name (`AppState::principal_scope`), so the same key from
    /// another principal is another record, and one principal cannot reach
    /// another's.
    pub(super) fn new(state: &'a SharedState, auth: &Auth, request: &IdempotentRequest) -> Self {
        let principal = state.principal_scope(auth.principal());
        Self { state, write: request.keyed_write(&principal) }
    }

    /// A storage error of the call, as the API answers it, counted when it is
    /// one of the two answers a key draws: `reused` and `in_flight`.
    pub(super) fn refused(&self, e: StorageError) -> ApiError {
        match e {
            StorageError::IdempotencyKeyReused => self.count(KeyedOutcome::Reused),
            StorageError::IdempotencyKeyInFlight => self.count(KeyedOutcome::InFlight),
            _ => {}
        }
        ApiError::from(e)
    }

    /// A fresh answer: the request was processed now. `resumed` when it
    /// continued an attempt of itself that had stopped.
    pub(super) fn fresh(&self, body: Value, resumed: bool) -> WriteAnswer {
        self.count(if resumed { KeyedOutcome::Resumed } else { KeyedOutcome::Applied });
        WriteAnswer::first(body)
    }

    /// The first answer, again, from `record`: marked as a replay, and counted
    /// as one, and as a peer's when another member wrote the record.
    pub(super) fn replayed(&self, record: &IdempotencyRecord, body: Value) -> WriteAnswer {
        self.count(KeyedOutcome::Replayed);
        if record.origin != self.state.engine.node_id() {
            self.state.metrics.record_idempotency_peer_replay();
        }
        WriteAnswer::replay(body)
    }

    fn count(&self, outcome: KeyedOutcome) {
        self.state.metrics.record_idempotency(outcome);
    }
}

/// A record whose outcome is not of the route that found it. The engine replays
/// a record only to its own kind and refuses any other as a key reused, so this
/// is a record that decoded to something its kind cannot hold: a fault, never
/// the client's.
pub(super) fn mismatched(record: &IdempotencyRecord) -> ApiError {
    // The origin and not the outcome: a `find_and_modify` outcome holds a
    // document, which a log line is no place for.
    tracing::error!(
        origin = %record.origin,
        "an idempotency record's outcome is not of the route that replays it"
    );
    ApiError::internal("an idempotency record could not be replayed")
}
