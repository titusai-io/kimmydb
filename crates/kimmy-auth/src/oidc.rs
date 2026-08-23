//! Verifying tokens an external OpenID Connect provider issued.
//!
//! This is a *second* verifier beside [`TokenIssuer`](crate::token::TokenIssuer),
//! not a replacement for it. A token is only ever offered to the verifier it
//! claims to belong to — the caller reads the unverified `iss` with
//! [`claimed_issuer`] and routes on it — so there is no algorithm-confusion
//! surface between the two: a local HS256 token never reaches this code, and an
//! RS256 token never reaches the HS256 path. Each verifier then re-checks the
//! algorithm against its own fixed list, so the routing being wrong would still
//! not make either of them accept a token signed the other way. See ADR-064.
//!
//! # It does no I/O, exactly like `token.rs`
//!
//! The key set is **injected**. Fetching it — discovery, JWKS, refresh on a
//! timer — happens in `kimmyd`, and this crate never learns that an identity
//! provider is something you reach over a network. That property is what keeps
//! authentication free on the request path, and it is why this file has no
//! `async` in it.

use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{AuthError, Result};
use crate::rbac::{Action, Grant, Principal};

/// Clock skew tolerated on a federated token's expiry.
///
/// The local HS256 path allows **none** (`token.rs`), and that difference is
/// deliberate: nodes in one cluster are expected to agree about the time and
/// are operated by whoever operates the database, whereas an external identity
/// provider is somebody else's clock. A minute is the customary allowance, and
/// without it a fleet whose NTP has drifted by seconds refuses freshly minted
/// tokens as expired — a failure that looks like an outage in the IdP.
pub const OIDC_LEEWAY_SECS: u64 = 60;

/// Signature algorithms a federated token may use.
///
/// Asymmetric only, and named rather than derived from the token: an identity
/// provider signs with a private key nobody here holds, so a symmetric
/// algorithm arriving on this path can only be an attempt to have a *public*
/// key verified as though it were a shared secret.
pub const OIDC_ALGORITHMS: [Algorithm; 2] = [Algorithm::RS256, Algorithm::ES256];

/// One value of the roles claim, and what holding it grants.
///
/// Grants are written inline in the configuration file rather than looked up in
/// a table this database owns (ADR-066), so the whole mapping is one thing an
/// operator can read and diff.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RoleMapping {
    /// The value that must appear in the roles claim, verbatim.
    pub claim_value: String,
    /// What that value is worth here.
    pub grants: Vec<Grant>,
}

/// What this node trusts about one external issuer.
///
/// One issuer, not a list. A second issuer means a second trust root, and the
/// question "which of these may say a subject is an analyst" has no answer this
/// round — see ADR-064.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OidcSettings {
    /// The `iss` a token must carry, matched exactly.
    pub issuer: String,
    /// The `aud` a token must carry. Without it any token the provider ever
    /// issued — including one minted for an unrelated application — would be
    /// accepted here.
    pub audience: String,
    /// Claim carrying the caller's roles. `roles` for most providers,
    /// `groups` for Entra ID.
    pub roles_claim: String,
    /// Claim values, and the grants they carry.
    pub role_mappings: Vec<RoleMapping>,
}

impl OidcSettings {
    /// Refuse settings that cannot mean what they say.
    ///
    /// Called by `Config::validate` as well as by [`OidcVerifier::new`], for the
    /// same reason `MIN_SECRET_LEN` is public: `check-config` must refuse
    /// exactly what the server would refuse, and an entrypoint check that
    /// blesses a configuration the node then rejects is worse than no check.
    pub fn validate(&self) -> Result<()> {
        for mapping in &self.role_mappings {
            // `admin` is reserved to local users as a break-glass boundary
            // (ADR-067). If the identity provider is misconfigured or taken
            // over, nobody gets superuser over KimmyDB through it — so a
            // mapping that would hand it out stops the node at startup rather
            // than becoming a privilege the operator discovers afterwards.
            if mapping.grants.iter().any(|g| g.actions.contains(&Action::Admin)) {
                return Err(AuthError::AdminNotFederatable {
                    claim_value: mapping.claim_value.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Verifies tokens from one external issuer against an injected key set.
#[derive(Clone, Debug)]
pub struct OidcVerifier {
    settings: OidcSettings,
    keys: JwkSet,
}

impl OidcVerifier {
    /// A verifier with no keys yet.
    ///
    /// Starting empty is the normal case, not a degraded one: the key set
    /// arrives from the provider after the node is already serving, because a
    /// briefly unreachable IdP must not stop a database from restarting. Until
    /// it lands, every federated token is refused with
    /// [`AuthError::UnknownSigningKey`], which is also what asks the refresher
    /// to try again.
    pub fn new(settings: OidcSettings) -> Result<Self> {
        settings.validate()?;
        Ok(Self { settings, keys: JwkSet { keys: Vec::new() } })
    }

    /// The same verifier with a different key set, as key rotation produces.
    pub fn with_keys(&self, keys: JwkSet) -> Self {
        Self { settings: self.settings.clone(), keys }
    }

    pub fn settings(&self) -> &OidcSettings {
        &self.settings
    }

    pub fn issuer(&self) -> &str {
        &self.settings.issuer
    }

    /// How many signing keys this verifier currently holds.
    pub fn key_count(&self) -> usize {
        self.keys.keys.len()
    }

    /// Whether the token *claims* to come from this provider.
    ///
    /// Reads an unverified claim, which is safe only because of what it is used
    /// for: choosing which verifier gets to say yes. A forged `iss` routes an
    /// attacker's token to a verifier that then refuses it, and omitting `iss`
    /// routes it to the local one, which refuses it just as firmly. There is no
    /// third outcome, because both verifiers demand their own signature.
    pub fn claims_this_issuer(&self, token: &str) -> bool {
        claimed_issuer(token).as_deref() == Some(self.settings.issuer.as_str())
    }

    /// Verify a token and recover the principal it authorizes.
    ///
    /// Signature, issuer, audience and expiry. No storage is consulted and none
    /// exists to consult: a federated subject has no local user record, which
    /// is also why token-version revocation does not apply to it (ADR-065).
    pub fn verify(&self, token: &str) -> Result<Principal> {
        let header = decode_header(token).map_err(|_| AuthError::InvalidToken)?;
        if !OIDC_ALGORITHMS.contains(&header.alg) {
            return Err(AuthError::InvalidToken);
        }

        let jwk = self.key_for(header.kid.as_deref())?;
        let key = DecodingKey::from_jwk(jwk).map_err(|_| AuthError::InvalidToken)?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.settings.issuer]);
        validation.set_audience(&[&self.settings.audience]);
        // Presence is required explicitly. Without this, `iss` and `aud` are
        // only checked when the token *carries* them, so a token that simply
        // omits its audience would sail past the audience restriction — which
        // is the whole reason the audience is configured.
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_exp = true;
        validation.leeway = OIDC_LEEWAY_SECS;

        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::TokenExpired,
                _ => AuthError::InvalidToken,
            })?
            .claims;

        let subject = claims.get("sub").and_then(Value::as_str).ok_or(AuthError::InvalidToken)?;
        Ok(Principal::federated(subject, self.grants_for(&claims)))
    }

    /// The key a token's `kid` names.
    ///
    /// A `kid` nobody knows is reported as such rather than as a plain refusal,
    /// because it is the ordinary consequence of the provider rotating its keys
    /// and the caller can act on it — one rate-limited refetch, and the next
    /// request succeeds. Reported the same way when the set is still empty,
    /// which is what makes a node that started before its IdP was reachable
    /// recover by itself.
    fn key_for(&self, kid: Option<&str>) -> Result<&Jwk> {
        match kid {
            Some(kid) => {
                self.keys.find(kid).ok_or_else(|| AuthError::UnknownSigningKey(kid.to_string()))
            }
            // A single-key set is unambiguous, so a provider that omits `kid`
            // still works. With several keys there is nothing to choose by, and
            // trying each in turn would make a signature check into a search.
            None if self.keys.keys.len() == 1 => Ok(&self.keys.keys[0]),
            None => Err(AuthError::InvalidToken),
        }
    }

    /// The grants the claims earn, which may be none at all.
    ///
    /// A role nobody mapped contributes nothing, so an unmapped identity
    /// becomes a principal with zero grants rather than an error: it
    /// authenticated, it is simply not authorized for anything here, and
    /// `Principal::can` already answers `false` to every question for such a
    /// principal. Refusing at the door instead would turn "your administrator
    /// has not given you access to this database" into "your login is broken".
    fn grants_for(&self, claims: &Value) -> Vec<Grant> {
        let held = roles(claims, &self.settings.roles_claim);
        self.settings
            .role_mappings
            .iter()
            .filter(|m| held.iter().any(|r| r == &m.claim_value))
            .flat_map(|m| m.grants.iter().cloned())
            .collect()
    }
}

/// The issuer a token claims, with nothing verified.
///
/// Used to decide which verifier a token belongs to, and for nothing else. See
/// [`OidcVerifier::claims_this_issuer`].
pub fn claimed_issuer(token: &str) -> Option<String> {
    let data = jsonwebtoken::dangerous::insecure_decode::<Value>(token).ok()?;
    data.claims.get("iss")?.as_str().map(str::to_string)
}

/// The role values a token carries, from the configured claim.
///
/// Two shapes, because providers disagree: a JSON array of strings (`roles`,
/// `groups`), and a single space-separated string (the shape `scope` uses, and
/// what some providers emit for a lone role). Anything else contributes no
/// roles, which costs the caller their grants rather than their login.
fn roles(claims: &Value, claim: &str) -> Vec<String> {
    match claims.get(claim) {
        Some(Value::Array(values)) => {
            values.iter().filter_map(Value::as_str).map(str::to_string).collect()
        }
        Some(Value::String(value)) => value.split_whitespace().map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use jsonwebtoken::{EncodingKey, Header};
    use serde_json::json;

    use super::*;

    const ISSUER: &str = "https://auth.example.com";
    const AUDIENCE: &str = "kimmydb";
    const KID: &str = "test-key-1";

    /// A PKCS#1 RSA private key, fixed so the tests are deterministic and cost
    /// no key generation. It signs nothing outside this file.
    const RSA_DER_B64: &str = concat!(
        "MIIEowIBAAKCAQEAskSt8G8fV8ZteU7D70PGq2y5Q/dtHnxjsnGfr054ja/rNuLyQxmniWVi",
        "aLYLNujpZeHR32dxFivh/7sZO9RbsgW/umwV1HCO5EEDMIITwoERFyGxMOeWR8m8ow6KDHI4",
        "O0J1Y6maNqU6oBCtyhgy+6G91O6q8c2ZzPD7iwSgy1s9uaA88H4w4/sUJ0KIJXO6Kpgo/Qdt",
        "m+RHeKG4V/LYWevCfSP9hOULrEI2X9jIi/P1S4OcGj4ieTtF56TA/lYPVw5lBHtNORyliGq6",
        "s0knhOzx5DMlsrWBhv7cD8XgHpK0pU0TVkrUDf/KcCLd3H5uyw7k3RzaK670bmwF1bpskQID",
        "AQABAoIBAEcdBaQtt/ucYOxs4tWOHHEi+I7n44QvS9gR4okczRN8c2DcTJc+4yn4ozaxNC0N",
        "4ZluaXnsulyFWezZlrnav093YqH73wN1eVMNqjeOFFLZiNdI7fXb1IPDsrf7I0/OuqbNHqYI",
        "sMeOxyG2NZWybJgbz+3i3ZeDFJEAKuAskvY7vYfjx5kznpPyfQqWX3inz7uY9Ptks02OPCV2",
        "MZ4sVbz8xPy42ST5sRzhH+2XFSktgJ9uIQZN3jgTQhoS/FSwMLnMI0UVjJeWMdZgVKgSYbn6",
        "qWBxWI62yulxitmO+X3p8ZtwIT8LhZdhCe1tZeoKg9Z+HDvX6o5y1gEf2RwA3o8CgYEA+g1W",
        "17AZIIQPB4KOpqG4wPiqJEhYxk5vV2g2bdm0i+lIoUBrQSMJnKSUQxpxVoabYyp6sYw4MZln",
        "wCoUIGSHU4zN6C2qXXFYVIHkLeN2p9NYToMQGkTCmNioAvCmvyPTS+C9z7yi4m9gSXsZ+Gx9",
        "veWb6Thfq5+WUnQn06q9oMcCgYEAtoI4voB8pUDWHDtOXtVtlSPhkSLJrsa90YVHmlWbR8pc",
        "vA9wi3twznuXXJzNrq4x0gw1nh5EVMOhToeBusX+hlhHKktb1MH3lXY7eT+FPbS9Wu521zKi",
        "dysnofJq3YrVlVFzeOWe3r8NofE1Lr8prVO1dFnuQsdxPfJbFm7n3+cCgYBykOgIHKP2lOr5",
        "6uSHDjPDHmt+AjPCcC9tYc8GV6f0LqdbUlOR3YbK4VEYyaXCGhxZvB3I+VDJ0NqLXfwot0aV",
        "jj7NMRcMhyEMXxL3v28fB6M/HaekEXsDYsjfx/juPHDUJB1zb59FlfgM0r0caEDYX7omifCz",
        "hoPuNVAGGAWYAwKBgEV2NICUyFvg3FysWbyQQH/Fw0EI23fQnkgTENh1gn8FTtwoiC4eEiYU",
        "NdyCtWmpVL7b9MA0Rs94EXmg60gZuTCKgrNfMRk9paxV7nbMLTr6AiOMpOBsnhb67r+dUvz0",
        "rSuCb49w3VFrp5WeBx6+lO8p7+LTo3H5FGl+Rxq3pTq7AoGBALsBIzjsuI1IRkE9Kz9209bn",
        "KYums/29sapgmfQ2fti/5aWqFncyspiOpHTR2hTELvTj7jEV2QK4Cm0COkrUfl793o+Tip/1",
        "3REQLSnnc16iJ3E95ZVlVwDsadWfoGlJbF42ij4k0wb08j7SD05pnZiujr8nMW9yPq4OZY9F",
        "UPAu",
    );

    /// A PKCS#8 P-256 private key, for the ES256 half of the accepted set.
    const EC_DER_B64: &str = concat!(
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgdYt6Sm2yyfFR8Bic5yJIzy6A",
        "Ra59sojVUjw/3t5rwyOhRANCAASTbia99nDdIMlZG1ND4yE0aYr4lybfQbD2whxMikG8lbsH",
        "O6OtfLKUpjzwvieZriD+AhtalEtnc1pXO6GvNSrL",
    );

    fn der(b64: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode(b64).expect("valid base64")
    }

    fn signing_key(alg: Algorithm) -> EncodingKey {
        match alg {
            Algorithm::RS256 => EncodingKey::from_rsa_der(&der(RSA_DER_B64)),
            Algorithm::ES256 => EncodingKey::from_ec_der(&der(EC_DER_B64)),
            other => panic!("no test key for {other:?}"),
        }
    }

    /// The public half of a signing key, as a provider would publish it.
    fn jwks(alg: Algorithm, kid: &str) -> JwkSet {
        let mut jwk = Jwk::from_encoding_key(&signing_key(alg), alg).expect("public components");
        jwk.common.key_id = Some(kid.to_string());
        JwkSet { keys: vec![jwk] }
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Mint a token the way the identity provider would.
    fn sign(alg: Algorithm, kid: Option<&str>, claims: Value) -> String {
        let mut header = Header::new(alg);
        header.kid = kid.map(str::to_string);
        jsonwebtoken::encode(&header, &claims, &signing_key(alg)).expect("signed")
    }

    fn claims(roles: Value) -> Value {
        json!({
            "sub": "ada@example.com",
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": now() + 3600,
            "iat": now(),
            "roles": roles,
        })
    }

    fn settings() -> OidcSettings {
        OidcSettings {
            issuer: ISSUER.into(),
            audience: AUDIENCE.into(),
            roles_claim: "roles".into(),
            role_mappings: vec![RoleMapping {
                claim_value: "kimmydb-analyst".into(),
                grants: vec![Grant::new("sales", "orders*", vec![Action::Read, Action::Search])],
            }],
        }
    }

    fn verifier(alg: Algorithm) -> OidcVerifier {
        OidcVerifier::new(settings()).unwrap().with_keys(jwks(alg, KID))
    }

    #[test]
    fn a_mapped_role_becomes_the_grants_it_names() {
        for alg in OIDC_ALGORITHMS {
            let token = sign(alg, Some(KID), claims(json!(["kimmydb-analyst"])));
            let principal = verifier(alg).verify(&token).unwrap_or_else(|e| panic!("{alg:?}: {e}"));

            assert_eq!(principal.user, "ada@example.com");
            assert!(principal.can(Action::Read, "sales", Some("orders_2024")));
            assert!(principal.can(Action::Search, "sales", Some("orders")));
            assert!(!principal.can(Action::Write, "sales", Some("orders")));
        }
    }

    #[test]
    fn a_federated_principal_is_marked_as_one() {
        // The audit log has to be able to say "somebody the IdP called ada"
        // rather than "ada", and revocation reads the same flag: there is no
        // local record to carry a token version (ADR-065).
        let token = sign(Algorithm::RS256, Some(KID), claims(json!(["kimmydb-analyst"])));
        let principal = verifier(Algorithm::RS256).verify(&token).unwrap();

        assert!(principal.federated);
        assert!(!principal.unauthenticated);
        assert_eq!(principal.token_version, 0, "a federated token versions nothing");
    }

    #[test]
    fn roles_that_map_to_nothing_produce_a_principal_with_no_grants() {
        // Authenticated but not authorized. Refusing the token instead would
        // report "your login is broken" for what is really "your administrator
        // has not given you access to this database".
        let token = sign(Algorithm::RS256, Some(KID), claims(json!(["some-other-app-role"])));
        let principal = verifier(Algorithm::RS256).verify(&token).unwrap();

        assert_eq!(principal.user, "ada@example.com");
        assert!(principal.grants.is_empty());
        assert!(!principal.can(Action::Read, "sales", Some("orders")));
        assert!(!principal.can(Action::Read, "sales", None));
    }

    #[test]
    fn a_token_from_another_issuer_is_rejected() {
        let mut other = claims(json!(["kimmydb-analyst"]));
        other["iss"] = json!("https://evil.example.com");
        let token = sign(Algorithm::RS256, Some(KID), other);

        assert!(matches!(verifier(Algorithm::RS256).verify(&token), Err(AuthError::InvalidToken)));
        // ...and it is not even offered to this verifier, because routing reads
        // the same claim.
        assert!(!verifier(Algorithm::RS256).claims_this_issuer(&token));
    }

    #[test]
    fn a_token_for_another_audience_is_rejected() {
        // The provider signs tokens for every application that trusts it. Only
        // the audience separates a token minted for the wiki from one minted
        // for this database.
        let mut other = claims(json!(["kimmydb-analyst"]));
        other["aud"] = json!("some-other-app");
        let token = sign(Algorithm::RS256, Some(KID), other);

        assert!(matches!(verifier(Algorithm::RS256).verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn a_token_with_no_audience_at_all_is_rejected() {
        // The quiet one: `aud` is only compared when it is present, so without
        // requiring it a token that simply omits the claim would pass the
        // audience restriction the operator configured.
        let mut bare = claims(json!(["kimmydb-analyst"]));
        bare.as_object_mut().unwrap().remove("aud");
        let token = sign(Algorithm::RS256, Some(KID), bare);

        assert!(matches!(verifier(Algorithm::RS256).verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn a_token_signed_by_a_key_the_provider_does_not_publish_is_rejected() {
        // Same `kid`, different key: the attacker's signature is checked
        // against the published public key and fails.
        let token = sign(Algorithm::ES256, Some(KID), claims(json!(["kimmydb-analyst"])));
        let rsa_only =
            OidcVerifier::new(settings()).unwrap().with_keys(jwks(Algorithm::RS256, KID));

        assert!(rsa_only.verify(&token).is_err());
    }

    #[test]
    fn an_hs256_token_is_refused_on_the_oidc_path() {
        // Algorithm confusion, the direction that matters most: the provider's
        // *public* key is public, so an HS256 token accepted here would be one
        // anybody could mint from published material.
        let secret = jsonwebtoken::EncodingKey::from_secret(b"a-sufficiently-long-secret");
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(KID.to_string());
        let token =
            jsonwebtoken::encode(&header, &claims(json!(["kimmydb-analyst"])), &secret).unwrap();

        assert!(matches!(verifier(Algorithm::RS256).verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn the_none_algorithm_is_not_accepted() {
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(br#"{"alg":"none","typ":"JWT","kid":"test-key-1"}"#);
        let payload = b64.encode(serde_json::to_vec(&claims(json!(["kimmydb-analyst"]))).unwrap());

        assert!(verifier(Algorithm::RS256).verify(&format!("{header}.{payload}.")).is_err());
    }

    #[test]
    fn an_expired_token_is_reported_as_expired() {
        let mut stale = claims(json!(["kimmydb-analyst"]));
        // Well past the minute of leeway, so this is expiry rather than skew.
        stale["exp"] = json!(now() - 600);
        let token = sign(Algorithm::RS256, Some(KID), stale);

        assert!(matches!(verifier(Algorithm::RS256).verify(&token), Err(AuthError::TokenExpired)));
    }

    #[test]
    fn a_token_that_expired_within_the_leeway_is_still_accepted() {
        // The whole point of the allowance: the provider's clock is not this
        // cluster's, and a few seconds of drift must not read as an outage.
        let mut fresh = claims(json!(["kimmydb-analyst"]));
        fresh["exp"] = json!(now() - 5);
        let token = sign(Algorithm::RS256, Some(KID), fresh);

        assert!(verifier(Algorithm::RS256).verify(&token).is_ok());
        assert_eq!(OIDC_LEEWAY_SECS, 60, "the local HS256 path allows none; this one allows this");
    }

    #[test]
    fn an_unknown_key_id_is_reported_so_the_caller_can_refetch() {
        // Key rotation: the provider signs with a key it has published and this
        // node has not fetched yet. Distinguishable from a bad signature
        // precisely because the fix is automatic — refetch and retry — and
        // reporting it as a plain refusal would make rotation an outage.
        let token = sign(Algorithm::RS256, Some("rotated-key-2"), claims(json!([])));

        match verifier(Algorithm::RS256).verify(&token) {
            Err(AuthError::UnknownSigningKey(kid)) => assert_eq!(kid, "rotated-key-2"),
            other => panic!("expected an unknown-key report, got {other:?}"),
        }
    }

    #[test]
    fn a_rotated_key_works_once_the_new_key_set_is_installed() {
        let verifier = verifier(Algorithm::RS256);
        let token = sign(Algorithm::ES256, Some("rotated-key-2"), claims(json!([])));
        assert!(verifier.verify(&token).is_err(), "not yet fetched");

        let rotated = verifier.with_keys(jwks(Algorithm::ES256, "rotated-key-2"));
        assert!(rotated.verify(&token).is_ok(), "the swapped key set must be the one in use");
        // The old verifier is untouched, which is what makes the swap safe to
        // do under a live request.
        assert!(verifier.verify(&token).is_err());
    }

    #[test]
    fn an_empty_key_set_refuses_everything_without_panicking() {
        // The state a node is in between starting and first reaching its
        // provider. It must refuse, and it must ask for a refetch.
        let cold = OidcVerifier::new(settings()).unwrap();
        let token = sign(Algorithm::RS256, Some(KID), claims(json!([])));

        assert_eq!(cold.key_count(), 0);
        assert!(matches!(cold.verify(&token), Err(AuthError::UnknownSigningKey(_))));
    }

    #[test]
    fn a_provider_that_omits_the_key_id_works_only_with_one_key() {
        let token = sign(Algorithm::RS256, None, claims(json!(["kimmydb-analyst"])));
        assert!(verifier(Algorithm::RS256).verify(&token).is_ok());

        // With two keys there is nothing to choose by, and trying each in turn
        // would turn a signature check into a search.
        let mut two = jwks(Algorithm::RS256, KID);
        two.keys.extend(jwks(Algorithm::ES256, "second-key").keys);
        let ambiguous = OidcVerifier::new(settings()).unwrap().with_keys(two);
        assert!(matches!(ambiguous.verify(&token), Err(AuthError::InvalidToken)));
    }

    #[test]
    fn garbage_is_rejected_rather_than_panicking() {
        let verifier = verifier(Algorithm::RS256);
        for token in ["", "not.a.token", "a.b", "....", "eyJhbGciOiJSUzI1NiJ9"] {
            assert!(verifier.verify(token).is_err(), "{token:?} should be rejected");
            assert_eq!(claimed_issuer(token), None, "{token:?} names no issuer");
        }
    }

    #[test]
    fn a_role_mapping_that_grants_admin_is_refused() {
        // `admin` is reserved to local users as a break-glass boundary: if the
        // identity provider is misconfigured or compromised, nobody gets
        // superuser over KimmyDB through it (ADR-067).
        let mut settings = settings();
        settings.role_mappings.push(RoleMapping {
            claim_value: "kimmydb-admin".into(),
            grants: vec![Grant::superuser()],
        });

        match OidcVerifier::new(settings.clone()) {
            Err(AuthError::AdminNotFederatable { claim_value }) => {
                assert_eq!(claim_value, "kimmydb-admin");
            }
            other => panic!("expected a refusal, got {:?}", other.map(|_| "a verifier")),
        }
        // And the check the configuration layer calls agrees with the one the
        // verifier makes, so `check-config` cannot bless what the node refuses.
        assert!(settings.validate().is_err());
    }

    #[test]
    fn admin_is_refused_however_it_is_spelled_in_a_grant() {
        // Not only as a bare superuser grant: `admin` scoped to one collection
        // is still administration of that collection, including creating and
        // dropping it.
        let mut settings = settings();
        settings.role_mappings = vec![RoleMapping {
            claim_value: "sales-admin".into(),
            grants: vec![Grant::new("sales", "orders", vec![Action::Read, Action::Admin])],
        }];
        assert!(settings.validate().is_err());
    }

    #[test]
    fn every_other_action_is_mappable() {
        // The boundary is `admin` alone; federating a read-only or a
        // write-and-watch role is the point of the feature.
        let mut settings = settings();
        settings.role_mappings = vec![RoleMapping {
            claim_value: "editor".into(),
            grants: vec![Grant::new(
                "sales",
                "*",
                vec![Action::Read, Action::Write, Action::Watch, Action::Search, Action::Webhook],
            )],
        }];
        settings.validate().unwrap();
    }

    #[test]
    fn routing_reads_the_issuer_without_verifying_anything() {
        // What the extractor uses to pick a verifier. It must work on a token
        // this node cannot verify — that is the entire situation it exists for.
        let unsignable = sign(Algorithm::ES256, Some("whatever"), claims(json!([])));
        assert_eq!(claimed_issuer(&unsignable).as_deref(), Some(ISSUER));
        assert!(verifier(Algorithm::RS256).claims_this_issuer(&unsignable));

        // A local HS256 token carries no `iss`, so it routes to the local path.
        let local = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &json!({ "sub": "root", "exp": now() + 60 }),
            &EncodingKey::from_secret(b"a-sufficiently-long-secret"),
        )
        .unwrap();
        assert_eq!(claimed_issuer(&local), None);
        assert!(!verifier(Algorithm::RS256).claims_this_issuer(&local));
    }

    #[test]
    fn roles_are_read_from_the_configured_claim() {
        // Entra ID puts them in `groups`; most providers use `roles`. Reading
        // the wrong one silently costs every federated caller their grants.
        let mut settings = settings();
        settings.roles_claim = "groups".into();
        let verifier = OidcVerifier::new(settings).unwrap().with_keys(jwks(Algorithm::RS256, KID));

        let mut entra = claims(json!(["kimmydb-analyst"]));
        entra["groups"] = entra["roles"].take();
        entra.as_object_mut().unwrap().remove("roles");

        let principal = verifier.verify(&sign(Algorithm::RS256, Some(KID), entra)).unwrap();
        assert!(principal.can(Action::Read, "sales", Some("orders")));
    }

    #[test]
    fn a_space_separated_roles_claim_is_understood() {
        // Providers disagree about the shape. A string is what `scope` uses and
        // what some emit for a single role.
        assert_eq!(roles(&json!({ "roles": "a b  c" }), "roles"), vec!["a", "b", "c"]);
        assert_eq!(roles(&json!({ "roles": ["a", "b"] }), "roles"), vec!["a", "b"]);
        // A shape nobody expects costs grants, not the login.
        assert!(roles(&json!({ "roles": { "nested": true } }), "roles").is_empty());
        assert!(roles(&json!({}), "roles").is_empty());
    }

    #[test]
    fn several_mapped_roles_combine_their_grants() {
        let mut settings = settings();
        settings.role_mappings.push(RoleMapping {
            claim_value: "kimmydb-writer".into(),
            grants: vec![Grant::new("hr", "*", vec![Action::Write])],
        });
        let verifier = OidcVerifier::new(settings).unwrap().with_keys(jwks(Algorithm::RS256, KID));

        let token = sign(
            Algorithm::RS256,
            Some(KID),
            claims(json!(["kimmydb-analyst", "kimmydb-writer", "unmapped"])),
        );
        let principal = verifier.verify(&token).unwrap();

        assert!(principal.can(Action::Read, "sales", Some("orders")));
        assert!(principal.can(Action::Write, "hr", Some("people")));
        assert!(!principal.can(Action::Write, "sales", Some("orders")));
    }
}
