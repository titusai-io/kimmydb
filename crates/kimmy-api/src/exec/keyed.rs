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

#[cfg(test)]
mod tests {
    use super::*;
    use kimmy_core::{Fingerprint, Hlc, NodeId, ScopeHash, Stamp};
    use kimmy_storage::{RecordOutcome, RecordState};

    fn record(origin: NodeId) -> IdempotencyRecord {
        let stamp = Stamp::new(Hlc::new(1_760_000_000_000, 0), origin);
        IdempotencyRecord {
            fingerprint: Fingerprint([1; 32]),
            first: stamp,
            last: stamp,
            origin,
            state: RecordState::Done,
            outcome: RecordOutcome::Insert { stamp },
        }
    }

    fn read(state: &SharedState, series: &str) -> String {
        let page = state.metrics.render();
        page.lines().find(|l| l.starts_with(series)).unwrap_or_default().to_string()
    }

    /// A replay counts as one, and as a peer's only when another member wrote
    /// the record: the cross-member deduplication the series is about.
    #[test]
    fn a_replay_of_another_members_record_is_a_peer_replay() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::tests::a_state(&dir);
        let call = KeyedCall {
            state: &state,
            write: KeyedWrite::new(ScopeHash([2; 32]), Fingerprint([1; 32])),
        };
        let own = call.replayed(&record(state.engine.node_id()), Value::Null);
        assert!(own.replayed);
        assert_eq!(
            read(&state, "kimmy_idempotency_peer_replays_total"),
            "kimmy_idempotency_peer_replays_total 0"
        );
        call.replayed(&record(NodeId::from_bytes([9; 16])), Value::Null);
        assert_eq!(
            read(&state, "kimmy_idempotency_peer_replays_total"),
            "kimmy_idempotency_peer_replays_total 1"
        );
        assert_eq!(
            read(&state, "kimmy_idempotency_requests_total{outcome=\"replayed\"}"),
            "kimmy_idempotency_requests_total{outcome=\"replayed\"} 2"
        );
    }

    /// The two refusals a key draws are counted by name; any other storage
    /// error is answered as it always was and counted as neither.
    #[test]
    fn the_keys_own_refusals_are_counted_and_mapped() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::tests::a_state(&dir);
        let call = KeyedCall {
            state: &state,
            write: KeyedWrite::new(ScopeHash([2; 32]), Fingerprint([1; 32])),
        };
        let reused = call.refused(StorageError::IdempotencyKeyReused);
        assert_eq!((reused.status.as_u16(), reused.code.as_str()), (422, "idempotency_key_reused"));
        let in_flight = call.refused(StorageError::IdempotencyKeyInFlight);
        assert_eq!(
            (in_flight.status.as_u16(), in_flight.code.as_str()),
            (409, "idempotency_key_in_flight")
        );
        assert_eq!(in_flight.retry_after_secs, Some(1));
        let other = call.refused(StorageError::Stale { current: None });
        assert_eq!(other.code.as_str(), "stale");
        for (label, n) in [("reused", 1), ("in_flight", 1), ("applied", 0), ("replayed", 0)] {
            let series = format!("kimmy_idempotency_requests_total{{outcome=\"{label}\"}}");
            assert_eq!(read(&state, &series), format!("{series} {n}"));
        }
    }
}
