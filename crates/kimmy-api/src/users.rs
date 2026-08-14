//! User management routes.
//!
//! Managing users is a server-wide operation, so these require admin over `*`
//! rather than over any particular database. A grant scoped to one database
//! must not be able to mint a principal with wider reach than its holder.

use axum::extract::{Path, State};
use axum::{Json, http::StatusCode};
use kimmy_auth::{Action, Grant};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::json::JsonBody;
use crate::state::{Auth, SharedState};

/// The scope required to administer users.
fn require_server_admin(auth: &Auth) -> Result<(), ApiError> {
    auth.require(Action::Admin, "*", None)
}

#[derive(Deserialize)]
pub struct CreateUserRequest {
    user: String,
    password: String,
    #[serde(default)]
    grants: Vec<Grant>,
}

pub async fn create_user(
    State(state): State<SharedState>,
    auth: Auth,
    JsonBody(body): JsonBody<CreateUserRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_server_admin(&auth)?;

    // A password policy belongs here rather than at the edge, so every path
    // that creates a user is held to it.
    if body.password.len() < 8 {
        return Err(ApiError::bad_request("password must be at least 8 characters"));
    }

    let user = state.users.create(&state.engine, &body.user, &body.password, body.grants)?;
    Ok((StatusCode::CREATED, Json(json!({ "user": user.name, "grants": user.grants }))))
}

pub async fn list_users(
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    Ok(Json(json!({ "users": state.users.list(&state.engine)? })))
}

pub async fn get_user(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    match state.users.get(&state.engine, &name)? {
        // The password hash is deliberately not part of this response.
        Some(user) => Ok(Json(json!({
            "user": user.name,
            "grants": user.grants,
            "disabled": user.disabled,
        }))),
        None => Err(ApiError::not_found(format!("no user {name:?}"))),
    }
}

pub async fn delete_user(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;

    // Removing the last administrator would leave the server unadministrable
    // with no way back in short of editing the data directory.
    if state.users.list(&state.engine)?.len() <= 1 {
        return Err(ApiError::conflict("cannot delete the last remaining user"));
    }
    if name == auth.principal().user {
        return Err(ApiError::conflict("cannot delete the account you are signed in as"));
    }

    let deleted = state.users.delete(&state.engine, &name)?;
    // The account is gone, so the absence is the revocation — but only once
    // this node stops remembering the version it used to have (ADR-052).
    state.sessions.evict(&name);
    Ok(Json(json!({ "deleted": deleted })))
}

#[derive(Deserialize)]
pub struct PasswordRequest {
    password: String,
}

pub async fn set_password(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
    JsonBody(body): JsonBody<PasswordRequest>,
) -> Result<Json<Value>, ApiError> {
    // A user may always change their own password; changing anyone else's is
    // an administrative act.
    if name != auth.principal().user {
        require_server_admin(&auth)?;
    }
    if body.password.len() < 8 {
        return Err(ApiError::bad_request("password must be at least 8 characters"));
    }

    state.users.set_password(&state.engine, &name, &body.password)?;
    state.sessions.evict(&name);
    Ok(Json(json!({ "updated": name })))
}

#[derive(Deserialize)]
pub struct GrantsRequest {
    grants: Vec<Grant>,
}

pub async fn set_grants(
    State(state): State<SharedState>,
    auth: Auth,
    Path(name): Path<String>,
    JsonBody(body): JsonBody<GrantsRequest>,
) -> Result<Json<Value>, ApiError> {
    require_server_admin(&auth)?;
    state.users.set_grants(&state.engine, &name, body.grants)?;
    state.sessions.evict(&name);
    Ok(Json(json!({ "updated": name })))
}

/// Who am I, and what may I do?
pub async fn whoami(auth: Auth) -> Json<Value> {
    Json(json!({
        "user": auth.principal().user,
        "grants": auth.principal().grants,
        "authenticated": !auth.principal().unauthenticated,
    }))
}
