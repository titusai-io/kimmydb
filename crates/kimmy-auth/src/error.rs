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

    #[error("role {0:?} not found")]
    RoleNotFound(String),

    #[error("role {0:?} already exists")]
    RoleExists(String),

    /// Deleting the only user left would leave a server nobody can sign in
    /// to. Decided under the writer, in the transaction that would delete it.
    #[error("cannot delete the last remaining user")]
    LastUser,

    /// Disabling the only enabled user left would leave a server nobody can
    /// administer. Decided under the writer, in the transaction that would
    /// disable it.
    #[error("cannot disable the last remaining enabled user")]
    LastEnabledUser,

    #[error("the JWT secret must be at least {min} bytes")]
    WeakSecret { min: usize },

    /// The previous signing secret is the current one.
    ///
    /// Refused rather than ignored: the two-key window (ADR-101) exists so an
    /// operator can retire a secret, and a configuration naming the same value
    /// twice is a rotation that was edited halfway. Starting under it would
    /// report a rotation in progress when nothing had changed.
    #[error(
        "the previous JWT secret is the same as the current one, so nothing is being rotated; \
         set auth.jwt_secret to the new value and auth.jwt_previous_secret to the old one, or \
         remove auth.jwt_previous_secret"
    )]
    PreviousSecretIsCurrent,

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
         able to mint a superuser over this database. Grant read/write/watch/search/webhook/ddl \
         instead, and keep administration on a local account."
    )]
    AdminNotFederatable { claim_value: String },

    /// A role mapping names neither a stored role nor any inline grants.
    ///
    /// Refused at startup for the same reason a half-filled `[auth.oidc]`
    /// section is: a rule that can never grant anything is a typo, and the
    /// failure it produces otherwise is a caller who authenticates and is then
    /// authorized for nothing, with no indication that the config is at fault.
    #[error(
        "the OIDC role mapping for {claim_value:?} names neither `role` nor `grants`, so holding \
         that claim value would earn nothing. Set `role = \"<a role in this database>\"`, or \
         write the grants inline, or remove the mapping."
    )]
    EmptyRoleMapping { claim_value: String },

    /// The audience carries a URI scheme, so it names this node as an OAuth 2.0
    /// protected resource — but not as one a client could actually use.
    ///
    /// Refused at startup rather than silently ignored: an audience that looks
    /// like a resource identifier and is not a valid one would leave the node
    /// publishing metadata nothing can act on, which is worse than publishing
    /// none at all (ADR-071).
    #[error(
        "auth.oidc.audience is {audience:?}, which carries a URI scheme and so names this node \
         as an OAuth 2.0 protected resource — but {reason}. RFC 8707 §2 requires a resource \
         identifier to be an absolute URI without a fragment. It is what a client sends as its \
         `resource` parameter and what this node publishes at \
         /.well-known/oauth-protected-resource, so it has to be the public base URL clients \
         reach this node at. Use a bare string such as \"kimmydb\" instead if your provider \
         does not implement resource indicators."
    )]
    InvalidResourceIdentifier { audience: String, reason: String },

    #[error("password hashing failed: {0}")]
    Hashing(String),

    /// The user or role store's storage failed. Kept whole rather than turned
    /// into text, so that the API answers it as the storage error it is: a
    /// write whose commit failed after its fsync began is `outcome_unknown`,
    /// not an authentication failure to retry elsewhere.
    #[error("user or role store: {0}")]
    Storage(#[from] kimmy_storage::StorageError),

    #[error("could not issue token: {0}")]
    TokenIssue(String),
}
