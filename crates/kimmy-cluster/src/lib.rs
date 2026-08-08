//! Leaderless clustering for KimmyDB.
//!
//! SWIM membership via `foca`, DNS and Kubernetes headless-DNS discovery, and
//! version-vector-driven oplog anti-entropy.
//!
//! Anti-entropy replication works: [`discovery`] resolves peers, [`protocol`]
//! frames the wire, [`transport`] serves and syncs, and [`peers`] runs the
//! loop. SWIM membership via `foca` is still to come — until then a node syncs
//! with every address its seeds resolve to, without failure detection or
//! suspicion.

#![allow(dead_code)]

pub mod discovery;
pub mod peers;
pub mod protocol;
pub mod transport;

pub use discovery::{DEFAULT_GOSSIP_PORT, ResolveError, SeedSource};
pub use peers::{DEFAULT_DISCOVERY_INTERVAL, DEFAULT_SYNC_INTERVAL, ReplicationConfig, replicate};
pub use transport::{serve, sync_once};
