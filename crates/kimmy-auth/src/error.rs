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

    /// A federated token's own `exp − iat` is longer than this node accepts.
    ///
    /// Distinct from [`AuthError::InvalidToken`] because the caller can act on
    /// it and the reason is not sensitive: the provider minted a longer-lived
    /// token than `auth.oidc.max_token_lifetime_secs` admits, and either the
    /// provider's lifetime or this node's limit has to move. The message names
    /// the limit and nothing about the token (ADR-096).
    #[error(
        "the access token is valid for longer than the {max_secs} seconds this node accepts          (auth.oidc.max_token_lifetime_secs); shorten the provider's access token lifetime or          raise the limit"
    )]
    TokenLifetimeExceeded { max_secs: u64 },

    /// A federated token carries no `iat`, so its lifetime cannot be bounded.
    ///
    /// Refused rather than waved through, because the limit would otherwise be
    /// one omitted claim away from not applying. RFC 9068 §2.2 makes `iat`
    /// REQUIRED in a JWT access token, so a conforming provider never produces
    /// this (ADR-096).
    #[error(
        "the access token carries no iat, so its lifetime cannot be checked against the          {max_secs} seconds this node accepts (auth.oidc.max_token_lifetime_secs); RFC 9068          requires the claim"
    )]
    TokenLifetimeUnbounded { max_secs: u64 },

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

    /// `auth.oidc.max_token_lifetime_secs` is outside the range that means
    /// anything.
    ///
    /// Zero would refuse every token, and anything past a day is no longer a
    /// bound on the window ADR-073 describes but a decision not to have one —
    /// which is a decision this setting exists to prevent being made by typo
    /// (ADR-096).
    #[error(
        "auth.oidc.max_token_lifetime_secs is {secs}, which is outside 1..={max} seconds. It          bounds how long a federated token may be valid for by its own exp − iat, so that a          provider-side revocation is honoured within that many seconds; zero would refuse every          token, and more than a day is not a bound."
    )]
    InvalidTokenLifetimeLimit { secs: u64, max: u64 },

    #[error("password hashing failed: {0}")]
    Hashing(String),

    #[error("could not issue token: {0}")]
    TokenIssue(String),
}
