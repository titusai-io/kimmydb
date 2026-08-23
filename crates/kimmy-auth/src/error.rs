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

    /// The token names a signing key this node has not fetched.
    ///
    /// Separate from [`AuthError::InvalidToken`] because the caller can act on
    /// it: the identity provider rotated its keys, and one refetch of the JWKS
    /// makes the next request succeed. Collapsing it into a plain refusal would
    /// turn every rotation into an outage lasting until the next refresh tick.
    #[error("no signing key {0:?} in the identity provider's key set")]
    UnknownSigningKey(String),

    /// A role mapping would hand out `admin` from an IdP claim.
    ///
    /// Refused at startup, not at request time: `admin` is reserved to local
    /// users as a break-glass boundary, so that a misconfigured or compromised
    /// identity provider cannot produce a superuser over this database
    /// (ADR-067).
    #[error(
        "the OIDC role mapping for {claim_value:?} grants the `admin` action, which is reserved \
         to local users: an identity provider that is misconfigured or taken over must not be \
         able to mint a superuser over this database. Grant read/write/watch/search/webhook \
         instead, and keep administration on a local account."
    )]
    AdminNotFederatable { claim_value: String },

    #[error("password hashing failed: {0}")]
    Hashing(String),

    #[error("could not issue token: {0}")]
    TokenIssue(String),
}
