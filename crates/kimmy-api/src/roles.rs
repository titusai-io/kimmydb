//! Role management routes.
//!
//! The same bar as managing users, and for the same reason: a role is a set of
//! permissions, so whoever may edit one may hand out anything it names. A grant
//! scoped to one database must not be able to mint permissions with wider reach
//! than its holder, which is why these require `admin` over `*` rather than
//! over any particular database.
//!
//! # Editing a role revokes tokens
//!
//! Every route here that changes what a role is *worth* — [`set_grants`] and
//! [`delete_role`] — invalidates the outstanding tokens of everyone holding it.
//! Without that, a *narrowing* edit would do nothing until each holder's token
//! expired, silently contradicting the revocation promise
//! `POST /v1/users/{name}/grants` has made since ADR-052. Creating a role
//! cannot narrow anything, so it does not invalidate.
//!
//! The federated path needs none of this and gets none: a federated principal
//! has no user record and no token version, and its grants are resolved from
//! the role store on **every** request (ADR-073), so an edit already applies to
//! it immediately. What stays stale there is role *membership*, which lives in
//! the provider's token and is not re-read until that token expires.

use axum::extract::{Path, State};
use axum::{Json, http::StatusCode};
use kimmy_auth::Action;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::json::JsonBody;
use crate::state::{Auth, SharedState};
use crate::users::{GrantInput, grants};

/// The scope required to administer roles.
fn require_server_admin(auth: &Auth) -> Result<(), ApiError> {
    auth.require(Action::Admin, "*", None)
}

/// Bump the token version of every holder, and drop them from the session
/// cache so the bump is visible to the very next request.
/// Evict each holder a role edit invalidated from the session cache, after
/// the edit committed: the bump to each stored version is only half of a
/// revocation, because the session check reads this cache in front of it
/// (ADR-052).
fn evict(state: &SharedState, holders: &[String]) -> usize {
    for holder in holders {
        state.sessions.evict(holder);
    }
    holders.len()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRoleRequest {
    name: String,
    #[serde(default)]
    grants: Vec<GrantInput>,
}

pub async fn create_role(
    State(state): State<SharedState>,
    auth: Auth,
    JsonBody(body): JsonBody<CreateRoleRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_server_admin(&auth)?;
    let role = state.users.roles().create(&state.engine, &body.name, grants(body.grants))?;
    Ok((StatusCode::CREATED, Json(json!({ "role": role.name, "grants": role.grants }))))
}

pub async fn list_roles(
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    Ok(Json(json!({ "roles": state.users.roles().list(&state.engine)? })))
}

pub async fn get_role(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    match state.users.roles().get(&state.engine, &name)? {
        Some(role) => Ok(Json(json!({ "role": role.name, "grants": role.grants }))),
        None => Err(ApiError::not_found(format!("no role {name:?}"))),
    }
}

pub async fn delete_role(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    // The role and every holder's token version in one transaction
    // (ADR-192). Holders keep the name on their record, where it now resolves
    // to nothing: the alternative is rewriting every user record's roles on a
    // delete, and a dangling name that grants nothing is the safe direction
    // to fail in. It is also what lets a delete sent again reach them.
    let (deleted, holders) =
        kimmy_storage::blocking(|| state.users.delete_role(&state.engine, &name))?;
    let invalidated = evict(&state, &holders);
    Ok(Json(json!({ "deleted": deleted, "invalidated": invalidated })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantsRequest {
    grants: Vec<GrantInput>,
}

pub async fn set_role_grants(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
    JsonBody(body): JsonBody<GrantsRequest>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    let holders = kimmy_storage::blocking(|| {
        state.users.set_role_grants(&state.engine, &name, grants(body.grants))
    })?;
    let invalidated = evict(&state, &holders);
    Ok(Json(json!({ "updated": name, "invalidated": invalidated })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolesRequest {
    roles: Vec<String>,
}

/// Replace the roles one user holds.
///
/// A name that matches no role is accepted rather than refused. Roles and users
/// are administered independently and often by different people, so refusing
/// would make the order of two unrelated operations load-bearing; an unresolved
/// name simply grants nothing, which is the safe direction.
pub async fn set_user_roles(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
    JsonBody(body): JsonBody<RolesRequest>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    state.users.set_roles(&state.engine, &name, body.roles)?;
    // `set_roles` bumps this user's own token version; the cache in front of it
    // has to be told, exactly as it is when grants or a password change.
    state.sessions.evict(&name);
    Ok(Json(json!({ "updated": name })))
}
