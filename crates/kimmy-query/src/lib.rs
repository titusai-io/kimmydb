//! MongoDB-style query evaluation for KimmyDB.
//!
//! Filter documents are parsed into an AST once and then evaluated, rather than
//! being re-walked as BSON per document. The AST is also what the index planner
//! reads, so it is the shared representation rather than just a speed-up.

#![allow(dead_code)]

pub mod filter;

/// Dot-path resolution. Lives in `kimmy-core` because index maintenance in
/// `kimmy-storage` needs it too; re-exported here for convenience.
pub use kimmy_core::path;
pub mod shape;
pub mod update;

pub use filter::{Condition, Filter, matches};
pub use shape::{Projection, SortKey};
pub use update::Update;
