//! Authentication and authorization for KimmyDB.
//!
//! Local users with Argon2id password hashing, JWTs signed with a cluster-wide
//! secret, and per-database/per-collection RBAC. Optionally also **federated**
//! identities from an external OIDC provider, verified by a second verifier
//! that shares everything downstream of the token and nothing above it
//! (ADR-064).
//!
//! [`rbac::Principal::can`] is the single authorization decision point. Both
//! the HTTP API and the MCP server route through it, because a second
//! enforcement path is how an MCP tool ends up quietly more permissive than the
//! REST route beside it.

#![allow(dead_code)]

pub mod error;
pub mod oidc;
pub mod password;
pub mod rbac;
pub mod roles;
pub mod token;
pub mod users;

pub use error::{AuthError, Result};
/// The key-set types, re-exported so `kimmy-api` and `kimmyd` can carry a JWKS
/// without either of them taking a direct dependency on the JWT library. What
/// they hold is this crate's key material, not a third party's type.
pub use jsonwebtoken::jwk::{Jwk, JwkSet};
pub use oidc::{
    OIDC_LEEWAY_SECS, OidcSettings, OidcVerifier, PROTECTED_RESOURCE_METADATA_PATH, RoleMapping,
};
pub use rbac::{Action, Grant, Principal, Role};
pub use roles::{ROLES_COLLECTION, RoleStore};
pub use token::{Claims, MIN_SECRET_LEN, TokenIssuer};
pub use users::{SYSTEM_DB, USERS_COLLECTION, User, UserStore};
