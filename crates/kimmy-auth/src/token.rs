//! JWT issuance and verification.
//!
//! Tokens are signed with a cluster-wide secret so that any node can validate a
//! token any other node issued. That is a hard requirement for a leaderless
//! cluster: requests are not pinned to the node that logged the user in, so a
//! per-node key would produce intermittent 401s that only appear under load
//! balancing.

use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::error::{AuthError, Result};
use crate::rbac::{Grant, Principal};

/// The claims KimmyDB puts in a token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// Subject: the user name.
    pub sub: String,
    /// Expiry, seconds since the Unix epoch.
    pub exp: u64,
    /// Issued at.
    pub iat: u64,
    /// The grants in force for this token.
    ///
    /// Embedded rather than looked up per request, which keeps verification a
    /// pure function of the token. The cost is that a revoked or edited role
    /// only takes effect when the token expires — hence short lifetimes.
    #[serde(default)]
    pub grants: Vec<Grant>,
    /// The user's token version at the moment this token was issued.
    ///
    /// Checked against the user's current version by the caller, not here:
    /// this crate signs and decodes, and knows nothing about storage. See
    /// ADR-052. `default` so tokens issued before the field existed decode as
    /// 0, matching a user record that has never been bumped.
    #[serde(default)]
    pub tv: u64,
}

/// Signs and verifies tokens.
#[derive(Clone)]
pub struct TokenIssuer {
    encoding: EncodingKey,
    decoding: DecodingKey,
    ttl_secs: u64,
}

impl TokenIssuer {
    pub fn new(secret: &str, ttl_secs: u64) -> Result<Self> {
        // A short secret makes offline brute force cheap, and the whole cluster
        // shares this one value.
        const MIN_SECRET_LEN: usize = 16;
        if secret.len() < MIN_SECRET_LEN {
            return Err(AuthError::WeakSecret { min: MIN_SECRET_LEN });
        }
        Ok(Self {
            encoding: EncodingKey::from_secret(secret.as_bytes()),
            decoding: DecodingKey::from_secret(secret.as_bytes()),
            ttl_secs,
        })
    }

    /// How long an issued token lasts.
    ///
    /// Told to the client at login rather than left to be discovered by
    /// decoding the token: a bearer token is opaque to the protocol, and a
    /// client that has to parse one to know when to refresh is a client
    /// depending on a shape nothing promised it.
    pub fn ttl_secs(&self) -> u64 {
        self.ttl_secs
    }

    /// Issue a token for a principal.
    pub fn issue(&self, principal: &Principal) -> Result<String> {
        self.issue_at(principal, now_secs())
    }

    /// Issue a token as though it were `now`, so expiry is testable without
    /// sleeping.
    pub fn issue_at(&self, principal: &Principal, now: u64) -> Result<String> {
        let claims = Claims {
            sub: principal.user.clone(),
            iat: now,
            exp: now + self.ttl_secs,
            grants: principal.grants.clone(),
            tv: principal.token_version,
        };
        jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(|e| AuthError::TokenIssue(e.to_string()))
    }

    /// Verify a token and recover the principal it authorizes.
    ///
    /// Signature and expiry only: this stays a pure function with no engine and
    /// no I/O, which is what keeps authentication free. The recovered principal
    /// carries the token version it claims, and **checking that against the
    /// user's current version is the caller's job** — it needs storage, and
    /// this crate has none. See ADR-052.
    pub fn verify(&self, token: &str) -> Result<Principal> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        // No clock skew allowance: nodes in a cluster are expected to be
        // roughly in sync, and the HLC already tolerates skew for ordering.
        validation.leeway = 0;

        let data = jsonwebtoken::decode::<Claims>(token, &self.decoding, &validation).map_err(
            |e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken,
            },
        )?;

        Ok(Principal::new(data.claims.sub, data.claims.grants).at_version(data.claims.tv))
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbac::Action;

    const SECRET: &str = "a-sufficiently-long-test-secret";

    fn issuer() -> TokenIssuer {
        TokenIssuer::new(SECRET, 3600).unwrap()
    }

    fn analyst() -> Principal {
        Principal::new("analyst", vec![Grant::new("sales", "orders*", vec![Action::Read])])
    }

    #[test]
    fn a_token_round_trips_to_its_principal() {
        let issuer = issuer();
        let token = issuer.issue(&analyst()).unwrap();
        let recovered = issuer.verify(&token).unwrap();

        assert_eq!(recovered.user, "analyst");
        assert!(recovered.can(Action::Read, "sales", Some("orders")));
        assert!(!recovered.can(Action::Write, "sales", Some("orders")));
    }

    #[test]
    fn a_token_signed_with_another_secret_is_rejected() {
        let token = issuer().issue(&analyst()).unwrap();
        let other = TokenIssuer::new("a-completely-different-secret", 3600).unwrap();
        assert!(matches!(other.verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn every_node_sharing_the_secret_accepts_the_same_token() {
        // The requirement that makes a leaderless cluster work: a request may
        // land on any node, not the one that issued the token.
        let node_a = issuer();
        let node_b = TokenIssuer::new(SECRET, 3600).unwrap();
        let token = node_a.issue(&analyst()).unwrap();
        assert_eq!(node_b.verify(&token).unwrap().user, "analyst");
    }

    #[test]
    fn an_expired_token_is_rejected_and_reported_as_expired() {
        let issuer = issuer();
        // Issued far enough in the past that its one-hour life is over.
        let token = issuer.issue_at(&analyst(), now_secs() - 7200).unwrap();
        assert!(matches!(issuer.verify(&token), Err(AuthError::TokenExpired)));
    }

    #[test]
    fn a_token_expiring_shortly_is_still_valid() {
        let issuer = TokenIssuer::new(SECRET, 3600).unwrap();
        let token = issuer.issue_at(&analyst(), now_secs() - 3000).unwrap();
        assert!(issuer.verify(&token).is_ok());
    }

    #[test]
    fn a_tampered_token_is_rejected() {
        let issuer = issuer();
        let token = issuer.issue(&analyst()).unwrap();

        // Flip a character in the payload: the signature must no longer match.
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged_payload = {
            use base64::Engine as _;
            let claims = Claims {
                sub: "analyst".into(),
                iat: now_secs(),
                exp: now_secs() + 3600,
                grants: vec![Grant::superuser()],
                tv: 0,
            };
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&claims).unwrap())
        };
        parts[1] = &forged_payload;
        let forged = parts.join(".");

        assert!(
            issuer.verify(&forged).is_err(),
            "escalating grants by editing the payload must not verify"
        );
    }

    #[test]
    fn garbage_is_rejected_rather_than_panicking() {
        let issuer = issuer();
        for token in ["", "not.a.token", "a.b", "....", "eyJhbGciOiJIUzI1NiJ9"] {
            assert!(issuer.verify(token).is_err(), "{token:?} should be rejected");
        }
    }

    #[test]
    fn a_short_secret_is_refused() {
        // The whole cluster shares this value, so a weak one is a cluster-wide
        // weakness rather than a local one.
        assert!(matches!(TokenIssuer::new("short", 3600), Err(AuthError::WeakSecret { .. })));
        assert!(TokenIssuer::new("0123456789abcdef", 3600).is_ok());
    }

    #[test]
    fn the_none_algorithm_is_not_accepted() {
        // The classic JWT attack: an unsigned token claiming alg=none.
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let claims = Claims {
            sub: "root".into(),
            iat: now_secs(),
            exp: now_secs() + 3600,
            grants: vec![Grant::superuser()],
            tv: 0,
        };
        let payload = b64.encode(serde_json::to_vec(&claims).unwrap());
        let unsigned = format!("{header}.{payload}.");

        assert!(issuer().verify(&unsigned).is_err());
    }
}
