//! Embeddings and vector search for KimmyDB.
//!
//! Pluggable embedding providers, the oplog-driven embedding worker, and the
//! HNSW index behind the shadow `{collection}.__vectors` collections.
//! Landing in M2.

#![allow(dead_code)]
