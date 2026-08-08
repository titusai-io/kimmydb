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

pub mod discovery;
pub mod health;
pub mod membership;
pub mod peers;
pub mod protocol;
pub mod transport;

pub use discovery::{DEFAULT_CLUSTER_PORT, ResolveError, SeedSource};
pub use health::{DEFAULT_FANOUT, MAX_BACKOFF, PeerHealth};
pub use membership::{Member, Members, SeedFeed};
pub use peers::{DEFAULT_DISCOVERY_INTERVAL, DEFAULT_SYNC_INTERVAL, ReplicationConfig, replicate};
pub use transport::{serve, sync_once};
