//! Authentication and authorization for KimmyDB.
//!
//! Local users with Argon2id password hashing, JWTs signed with a cluster-wide
//! secret, and per-database/per-collection RBAC.
//!
//! [`rbac::Principal::can`] is the single authorization decision point. Both
//! the HTTP API and the MCP server route through it, because a second
//! enforcement path is how an MCP tool ends up quietly more permissive than the
//! REST route beside it.

#![allow(dead_code)]

pub mod error;
pub mod password;
pub mod rbac;
pub mod token;
pub mod users;

pub use error::{AuthError, Result};
pub use rbac::{Action, Grant, Principal, Role};
pub use token::{Claims, TokenIssuer};
pub use users::{SYSTEM_DB, USERS_COLLECTION, User, UserStore};
