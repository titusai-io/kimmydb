//! Authentication and authorization errors.

use thiserror::Error;

pub type Result<T, E = AuthError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("invalid username or password")]
    InvalidCredentials,

    #[error("authentication token is invalid")]
    InvalidToken,

    #[error("authentication token has expired")]
    TokenExpired,

    #[error("not authorized to {action} {target}")]
    Forbidden { action: String, target: String },

    #[error("user {0:?} not found")]
    UserNotFound(String),

    #[error("user {0:?} already exists")]
    UserExists(String),

    #[error("the JWT secret must be at least {min} bytes")]
    WeakSecret { min: usize },

    #[error("password hashing failed: {0}")]
    Hashing(String),

    #[error("could not issue token: {0}")]
    TokenIssue(String),
}
