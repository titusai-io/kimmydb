#![allow(dead_code)] // the stub: the request's code does not call it yet
//! What a request resolved (ADR-272, decision 11; Q6.4 item 4). An aggregation does not run on one read snapshot: each stage reads
//! storage when it runs, so a pipeline that reads one name twice (the source and a `$lookup` of it, a `$facet` branch, a
//! sub-pipeline) could read two different physicals across a swap. The request therefore resolves each name it reads ONCE, at its
//! first use, and every later use of the name answers from the memo; and a sink that is about to write checks, after the last read
//! and before its first write, that no physical the request read has been buried since: if one has, nothing is written and the
//! request answers the retryable `503`. Burial is monotone, so unburied at the end means unburied throughout.
//!
//! The memo is per thread and per request, like the request's deadline: the synchronous part of an aggregation runs on one thread
//! under one [`Scope`], and a scope inside another (the pipeline is run from a scan and from a finish) joins the outer one.

use kimmy_storage::CollectionMeta;

use super::{ApiError, SharedState};

/// A request's resolution memo, for as long as it lives.
pub(super) struct Scope;

impl Scope {
    /// Begin the request's memo with its source, which the request resolved when it authorized the read. Joins a memo already begun.
    pub(super) fn begin(_db: &str, _name: &str, _meta: &CollectionMeta) -> Scope {
        Scope
    }
}

/// The physical this request already resolved `name` to, if it has.
pub(super) fn recall(_db: &str, _name: &str) -> Option<CollectionMeta> {
    None
}

/// Remember what this request resolved `name` to.
pub(super) fn remember(_db: &str, _name: &str, _meta: &CollectionMeta) {}

/// After the last read and before the first write: `503` if a physical this request read has been buried since it was resolved.
pub(super) fn end_check(_state: &SharedState) -> Result<(), ApiError> {
    Ok(())
}
