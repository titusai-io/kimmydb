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

/// Shortest signing secret `TokenIssuer` will accept.
///
/// A short secret makes offline brute force cheap, and the whole cluster shares
/// this one value. Public so a caller can refuse a bad secret *before* building
/// an issuer — `kimmyd` checks it while validating configuration, so
/// `check-config` answers the same question the server would.
pub const MIN_SECRET_LEN: usize = 16;

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
    /// The named roles the user held when this token was issued.
    ///
    /// Carried for the audit record, not for authorization: the grants those
    /// roles resolved to are already in `grants` above, unioned with the user's
    /// direct ones (ADR-073). Embedding the names too is what lets an audit
    /// reader answer "which roles were in play" without a lookup, and it costs
    /// one storage read at login instead of one per request.
    ///
    /// A stale list, deliberately, in exactly the way `grants` is: an edit to
    /// the user's roles bumps `tv` and invalidates the token, so the two can
    /// never drift apart within one token's life.
    #[serde(default)]
    pub roles: Vec<String>,
}

/// Signs and verifies tokens.
///
/// Holds one signing key and, during a rotation, one more verifying key
/// (ADR-101). Every token this issuer mints is signed with the current secret;
/// a token is accepted if either secret verifies it. That is what lets an
/// operator change `KIMMY_JWT_SECRET` without ending every session at once:
/// the old secret rides along as the previous one for a token lifetime, and is
/// then removed.
#[derive(Clone)]
pub struct TokenIssuer {
    encoding: EncodingKey,
    decoding: DecodingKey,
    /// The secret before the current one, kept only for verification.
    previous: Option<DecodingKey>,
    ttl_secs: u64,
}

impl TokenIssuer {
    pub fn new(secret: &str, ttl_secs: u64) -> Result<Self> {
        Self::with_previous(secret, None, ttl_secs)
    }

    /// An issuer that signs with `secret` and also accepts tokens signed with
    /// `previous`.
    ///
    /// The previous secret is held to the same floor as the current one: it
    /// still verifies tokens, so a weak one is exactly as forgeable as a weak
    /// current key. It must also differ from the current one — the same value
    /// twice is not a rotation, it is a configuration that was edited halfway,
    /// and starting under it would let an operator believe a rotation happened
    /// when nothing changed.
    pub fn with_previous(secret: &str, previous: Option<&str>, ttl_secs: u64) -> Result<Self> {
        if secret.len() < MIN_SECRET_LEN {
            return Err(AuthError::WeakSecret { min: MIN_SECRET_LEN });
        }
        let previous = match previous {
            None => None,
            Some(prev) if prev.len() < MIN_SECRET_LEN => {
                return Err(AuthError::WeakSecret { min: MIN_SECRET_LEN });
            }
            Some(prev) if prev == secret => return Err(AuthError::PreviousSecretIsCurrent),
            Some(prev) => Some(DecodingKey::from_secret(prev.as_bytes())),
        };
        Ok(Self {
            encoding: EncodingKey::from_secret(secret.as_bytes()),
            decoding: DecodingKey::from_secret(secret.as_bytes()),
            previous,
            ttl_secs,
        })
    }

    /// Whether a previous secret is configured — that is, whether a rotation
    /// window is open. Never the value.
    pub fn has_previous_secret(&self) -> bool {
        self.previous.is_some()
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
            roles: principal.roles.clone(),
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
    ///
    /// The current secret is tried first and the previous one only if the
    /// current one finds the signature wrong. An expired token is reported as
    /// expired by whichever key verified its signature — that answer is final,
    /// because a token the current key signed was never signed by the previous
    /// one. A token neither key accepts is simply invalid, with nothing said
    /// about which keys were tried: a caller holding a token from a secret
    /// this cluster has retired is a caller with a bad token.
    pub fn verify(&self, token: &str) -> Result<Principal> {
        let claims = match (decode_with(&self.decoding, token), &self.previous) {
            (Err(AuthError::InvalidToken), Some(previous)) => decode_with(previous, token)?,
            (outcome, _) => outcome?,
        };

        Ok(Principal::new(claims.sub, claims.grants).at_version(claims.tv).with_roles(claims.roles))
    }
}

/// Decode and validate a token against one key.
fn decode_with(key: &DecodingKey, token: &str) -> Result<Claims> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    // No clock skew allowance: nodes in a cluster are expected to be
    // roughly in sync, and the HLC already tolerates skew for ordering.
    validation.leeway = 0;

    jsonwebtoken::decode::<Claims>(token, key, &validation).map(|data| data.claims).map_err(|e| {
        match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
            _ => AuthError::InvalidToken,
        }
    })
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

    /// The reported lifetime is the configured one, and it is the token's own.
    ///
    /// `expiresIn` is how a client decides when to refresh (ADR-059), so a
    /// wrong answer here is a client that renews too late and gets a 401, or
    /// renews constantly. It was asserted only in `kimmy-api`'s suite, which
    /// left it unpinned for any mutation run scoped to this crate — and this
    /// accessor is this crate's surface, so this crate should hold it.
    #[test]
    fn the_reported_lifetime_is_the_one_tokens_are_issued_with() {
        let issuer = TokenIssuer::new(SECRET, 900).unwrap();
        assert_eq!(issuer.ttl_secs(), 900);

        // And it is not merely stored: it is the `exp` a verifier will enforce.
        // Issued at a fixed past instant so the arithmetic is exact, which
        // means expiry validation has to be off — the subject is the claim's
        // value, not whether it is still live.
        let token = issuer.issue_at(&analyst(), 1_000).unwrap();
        let mut validation = jsonwebtoken::Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        let claims =
            jsonwebtoken::decode::<Claims>(&token, &issuer.decoding, &validation).unwrap().claims;
        assert_eq!(claims.exp - claims.iat, issuer.ttl_secs(), "told and issued must agree");
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
                roles: Vec::new(),
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

    const OLD: &str = "the-secret-being-rotated-out-of-use";
    const NEW: &str = "the-secret-being-rotated-into-use!";

    /// The rotation window (ADR-101): a token signed with the secret an
    /// operator is retiring keeps working on a node that names it as the
    /// previous one, and only there.
    #[test]
    fn a_token_signed_with_the_previous_secret_still_verifies() {
        let before = TokenIssuer::new(OLD, 3600).unwrap();
        let token = before.issue(&analyst()).unwrap();

        let rotating = TokenIssuer::with_previous(NEW, Some(OLD), 3600).unwrap();
        assert!(rotating.has_previous_secret());
        let recovered = rotating.verify(&token).expect("the previous secret must still verify");
        assert_eq!(recovered.user, "analyst");
        assert!(recovered.can(Action::Read, "sales", Some("orders")));

        // The same node without the previous secret is the situation after the
        // window closes: the token is just invalid.
        let rotated = TokenIssuer::new(NEW, 3600).unwrap();
        assert!(!rotated.has_previous_secret());
        assert!(matches!(rotated.verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn a_rotating_issuer_signs_with_the_current_secret_only() {
        // Signing with the current key is what makes the window close: once
        // every token the old key signed has expired, nothing depends on it.
        let rotating = TokenIssuer::with_previous(NEW, Some(OLD), 3600).unwrap();
        let token = rotating.issue(&analyst()).unwrap();

        assert!(TokenIssuer::new(NEW, 3600).unwrap().verify(&token).is_ok());
        assert!(matches!(
            TokenIssuer::new(OLD, 3600).unwrap().verify(&token),
            Err(AuthError::InvalidToken)
        ));
    }

    #[test]
    fn a_token_signed_with_neither_secret_is_invalid_during_a_rotation() {
        // Two accepted keys are not "any key": a third secret is refused with
        // the same error as before, and nothing says which keys were tried.
        const STRANGER: &str = "a-secret-this-cluster-never-held";

        let token = TokenIssuer::new(STRANGER, 3600).unwrap().issue(&analyst()).unwrap();
        let rotating = TokenIssuer::with_previous(NEW, Some(OLD), 3600).unwrap();
        assert!(matches!(rotating.verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn an_expired_token_is_expired_under_whichever_secret_signed_it() {
        // Expiry is judged by the key that verified the signature, and the
        // answer is not softened into "invalid" by trying the other key.
        let rotating = TokenIssuer::with_previous(NEW, Some(OLD), 3600).unwrap();
        let stale_old =
            TokenIssuer::new(OLD, 3600).unwrap().issue_at(&analyst(), now_secs() - 7200).unwrap();
        assert!(matches!(rotating.verify(&stale_old), Err(AuthError::TokenExpired)));
        let stale_new = rotating.issue_at(&analyst(), now_secs() - 7200).unwrap();
        assert!(matches!(rotating.verify(&stale_new), Err(AuthError::TokenExpired)));
    }

    #[test]
    fn a_previous_secret_equal_to_the_current_one_is_refused() {
        // Not a rotation: nothing changed, and a node that started would let
        // the operator believe otherwise.
        assert!(matches!(
            TokenIssuer::with_previous(SECRET, Some(SECRET), 3600),
            Err(AuthError::PreviousSecretIsCurrent)
        ));
    }

    #[test]
    fn a_short_previous_secret_is_refused() {
        // It still verifies tokens, so it is held to the same floor.
        assert!(matches!(
            TokenIssuer::with_previous(SECRET, Some("short"), 3600),
            Err(AuthError::WeakSecret { .. })
        ));
    }

    #[test]
    fn the_none_algorithm_is_not_accepted() {
        // The classic JWT attack: an unsigned token claiming alg=none.
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let claims = Claims {
            roles: Vec::new(),
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
