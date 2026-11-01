//! MongoDB-style query evaluation for KimmyDB.
//!
//! Filter documents are parsed into an AST once and then evaluated, rather than
//! being re-walked as BSON per document. The AST is also what the index planner
//! reads, so it is the shared representation rather than just a speed-up.

#![allow(dead_code)]

pub mod accumulators;
pub mod aggregate;
/// Cooperative cancellation at the request's deadline. Lives in `kimmy-core` so that
/// `kimmy-storage`, which does not depend on this crate, can check it too (ADR-266);
/// re-exported here, where the executor's loops use it.
pub use kimmy_core::deadline;
pub mod budget;
pub mod densify;
pub mod expr;
pub mod fill;
pub mod filter;
/// The `$jsonSchema` engine. Lives in `kimmy-core` so that `kimmy-storage` can parse a
/// validator without depending on this crate (ADR-181, ADR-257); re-exported here.
pub use kimmy_core::json_schema;

/// Dot-path resolution. Lives in `kimmy-core` because index maintenance in
/// `kimmy-storage` needs it too; re-exported here for convenience.
pub use kimmy_core::path;
pub mod plan;
pub mod shape;
pub mod stages;
pub mod update;
pub mod update_pipeline;
pub mod window;
pub mod window_acc;

#[cfg(test)]
mod accumulator_tests;
#[cfg(test)]
mod bucket_tests;
#[cfg(test)]
mod date_tests;
#[cfg(test)]
mod decimal_arith_tests;
#[cfg(test)]
mod densify_tests;
#[cfg(test)]
mod facet_tests;
#[cfg(test)]
mod fill_tests;
#[cfg(test)]
mod graph_tests;
#[cfg(test)]
mod json_schema_filter_tests;
#[cfg(test)]
mod math_tests;
#[cfg(test)]
mod positional_tests;
#[cfg(test)]
mod sink_tests;
#[cfg(test)]
mod sort_zip_tests;
#[cfg(test)]
mod stage_reference_tests;
#[cfg(test)]
mod stage_tests;
#[cfg(test)]
mod update_pipeline_tests;
#[cfg(test)]
mod window_deadline_tests;
#[cfg(test)]
mod window_tests;

pub use aggregate::{Accumulator, Limits, Stage};
pub use expr::{Expr, Op};
pub use filter::{ArrayPosition, Condition, Filter, matches, matches_at};
pub use json_schema::Schema;
pub use plan::{IndexPlan, choose};
pub use shape::{FindProjection, Projection, ProjectionPaths, SortKey};
pub use update::Update;
