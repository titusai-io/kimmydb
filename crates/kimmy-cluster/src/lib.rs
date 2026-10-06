//! Leaderless clustering for KimmyDB.
//!
//! SWIM membership via `foca`, DNS and Kubernetes headless-DNS discovery, and
//! version-vector-driven oplog anti-entropy.
//!
//! Two things gossip here, and they are different halves of the same idea.
//!
//! **State** travels by anti-entropy over TCP: [`discovery`] resolves peers,
//! [`protocol`] frames the wire, [`transport`] serves and syncs, and [`peers`]
//! runs the loop. Each node pulls what it lacks from a few peers per round, and
//! data reaches the cluster transitively.
//!
//! **Membership** travels by SWIM over UDP in [`membership`], so the cluster
//! forms a shared opinion about who is alive rather than each node forming its
//! own from failed connections.
//!
//! No leader, no election, no quorum, in either half.

#![allow(dead_code)]

pub mod catchup;
pub mod confirm;
pub mod discovery;
pub mod facts;
pub mod health;
pub mod membership;
pub mod peers;
pub mod protocol;
pub mod tls;
pub mod transport;
pub mod yielding;

#[cfg(test)]
mod codec_tests;

pub use confirm::{
    ConfirmConfig, ConfirmHook, ConfirmOutcome, Confirmer, PushSentHook, Resolution,
};
pub use discovery::{DEFAULT_CLUSTER_PORT, ResolveError, SeedSource, names_another_member};
pub use facts::{
    CatchUpReason, ClassState, Facts, FactsSource, MAX_TTL_COLLECTIONS, OwnerClass, PeerState,
    PerClass, StallCause, TtlHeld, Yielding, facts_undecodable_total,
};
pub use health::{DEFAULT_FANOUT, MAX_BACKOFF, PeerHealth, WARN_INTERVAL};
pub use membership::{Member, Members, PeerView, SeedFeed, ViewBlock};
pub use peers::{
    ContactEnd, DEFAULT_DISCOVERY_INTERVAL, DEFAULT_SYNC_INTERVAL, ENTRY_WAIT_BUCKETS_US,
    Histogram, LagVectors, MAX_PULLS_PER_CONTACT, PULL_BUCKETS_US, PullReport, ReplicationConfig,
    RoundHook, RoundReport, SELF_RECHECK, replicate,
};
pub use transport::{
    AcceptErrorHook, AcceptListener, FROZEN_CONTACTS, PeerPosition, PeerStalls, PushHook,
    REPAIR_ATTEMPTS, REPAIR_COOLDOWN_ROUNDS, Repair, ReplayCounters, ReplayResult, ServeFailHook,
    ServeFailure, accept_error_is_the_listeners, replay_counters, serve, serve_with, sync_once,
    sync_once_with,
};
