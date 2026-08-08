//! In-process MCP server for KimmyDB.
//!
//! Exposes collections to agents as MCP tools and resources over streamable
//! HTTP, mounted on the same axum router as the REST API and sharing its
//! storage handles — no separate process, no loopback hop.
//!
//! # The one rule
//!
//! **Every tool call runs as the authenticated principal, through the same
//! [`kimmy_auth::Principal::can`] the REST routes use.** This crate contains no
//! authorization logic of its own; it obtains a principal and hands it to
//! [`kimmy_api::exec`], where the check lives. Write tools are always
//! registered — a read-only token simply fails authorization when it calls one.
//! Capability is a property of the role, not of the build.
//!
//! That is the whole reason MCP lives in this process. A sidecar would need its
//! own copy of the rules, and a second copy is how an agent tool ends up quietly
//! more permissive than the route beside it.

#![allow(dead_code)]

mod auth;
mod resources;
mod tools;

pub use auth::mcp_router;
pub use tools::KimmyMcp;
