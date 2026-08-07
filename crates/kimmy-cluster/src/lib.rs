//! Leaderless clustering for KimmyDB.
//!
//! SWIM membership via `foca`, DNS and Kubernetes headless-DNS discovery, and
//! version-vector-driven oplog anti-entropy.
//!
//! Only [`discovery`] is implemented so far; membership and anti-entropy land
//! in M4.

#![allow(dead_code)]

pub mod discovery;

pub use discovery::{DEFAULT_GOSSIP_PORT, SeedSource};
