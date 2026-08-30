//! The live OIDC verifier, shared between the request path and the refresher.
//!
//! `kimmy-auth`'s [`OidcVerifier`] is immutable and does no I/O, which is what
//! keeps token verification a pure function. But an identity provider rotates
//! its signing keys, so *something* has to hold the current key set and swap it
//! without stopping the server. That is this: a read-mostly handle the
//! extractor reads per request and the refresher replaces wholesale.
//!
//! # Two triggers, one refetch
//!
//! A timer, because a provider that rotates on a schedule needs no prompting;
//! and a **nudge** raised when a token arrives naming a key id nobody knows,
//! because a rotation that happens between ticks would otherwise refuse every
//! request until the next one. The nudge is rate-limited, and that is not
//! politeness — the key id in a token is attacker-controlled, so an unlimited
//! nudge would be a way to make this node hammer its own identity provider.
//!
//! The swap costs nothing to a request in flight: a verifier already taken out
//! of the handle keeps working against the key set it was cloned with, exactly
//! as a TLS handshake finishes under the certificate it negotiated.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_auth::{AuthError, JwkSet, OidcVerifier, Principal};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tracing::debug;

/// Shortest gap between two key-id-driven refetches.
///
/// The bound on what an unknown `kid` can cost: whatever a caller sends, this
/// node asks its provider at most twice a minute on top of the timer. Not
/// configurable, because the number that matters is "not once per request" and
/// any value with that property is as good as any other.
pub const MIN_REFETCH_INTERVAL: Duration = Duration::from_secs(30);

/// The current verifier, plus the channel that asks for a fresher key set.
pub struct Federation {
    verifier: RwLock<Arc<OidcVerifier>>,
    /// Raised when a token named a key id the current set does not hold.
    refresh: Notify,
    /// When the last nudge was let through, for the rate limit above.
    last_nudge: Mutex<Option<Instant>>,
}

impl Federation {
    pub fn new(verifier: OidcVerifier) -> Arc<Self> {
        Arc::new(Self {
            verifier: RwLock::new(Arc::new(verifier)),
            refresh: Notify::new(),
            last_nudge: Mutex::new(None),
        })
    }

    /// The issuer this node federates with.
    pub fn issuer(&self) -> String {
        self.verifier.read().issuer().to_string()
    }

    /// What this node calls itself to an authorization server, when it can.
    ///
    /// `None` when the audience is an opaque string rather than an https URI —
    /// a perfectly ordinary configuration, and the one that shipped first. It
    /// means no RFC 9728 metadata and no `resource` parameter, not a fault.
    pub fn resource_identifier(&self) -> Option<String> {
        self.verifier.read().settings().resource_identifier().map(str::to_string)
    }

    /// Where this node's own RFC 9728 metadata lives, for the `resource_metadata`
    /// parameter of a `WWW-Authenticate` challenge (RFC 9728 §5.1).
    ///
    /// Built from the identifier rather than from the request's `Host`, because
    /// the identifier is the name the authorization server was told about; a
    /// challenge pointing anywhere else would send a client to look up a
    /// resource nobody issued a token for.
    pub fn resource_metadata_url(&self) -> Option<String> {
        self.resource_identifier().map(|resource| metadata_url_for(&resource))
    }

    /// The path that URL resolves to on this node, so the route can refuse a
    /// request for a *different* resource's metadata rather than answering for
    /// everything under the well-known prefix.
    pub fn resource_metadata_path(&self) -> Option<String> {
        self.resource_identifier().map(|resource| metadata_path_for(&resource))
    }

    /// The RFC 9728 protected resource metadata document.
    ///
    /// **No `scopes_supported`.** Authorization here is roles carried in the
    /// token, not scopes, and advertising a scope vocabulary would describe an
    /// access-control model this database does not implement — a client that
    /// asked for those scopes would get them and still be told no.
    pub fn protected_resource_metadata(&self) -> Option<serde_json::Value> {
        let verifier = self.verifier.read();
        let settings = verifier.settings();
        let resource = settings.resource_identifier()?;
        Some(serde_json::json!({
            "resource": resource,
            "authorization_servers": [settings.issuer],
            "bearer_methods_supported": ["header"],
        }))
    }

    /// Whether a federated principal may hold `admin` (ADR-074).
    ///
    /// Read per request rather than captured once, because it decides what a
    /// *stored* role is worth and stored roles change while the node runs.
    pub fn allow_federated_admin(&self) -> bool {
        self.verifier.read().settings().allow_federated_admin
    }

    /// How many signing keys are currently trusted. Zero until the first fetch
    /// lands, which is a state the node serves in rather than refusing to start.
    pub fn key_count(&self) -> usize {
        self.verifier.read().key_count()
    }

    /// Adopt a freshly fetched key set.
    pub fn install_keys(&self, keys: JwkSet) {
        let replacement = Arc::new(self.verifier.read().with_keys(keys));
        *self.verifier.write() = replacement;
    }

    /// Whether the token claims to come from this provider.
    pub fn claims_this_issuer(&self, token: &str) -> bool {
        self.verifier.read().claims_this_issuer(token)
    }

    /// Verify a federated token, asking for a refetch if it names a key id this
    /// node has not seen.
    ///
    /// The nudge lives here rather than at the call site so that every path
    /// that verifies a federated token gets rotation recovery, and so the rate
    /// limit has one place to be enforced.
    pub fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        // Cloned out of the lock before verifying, so a swap cannot block a
        // request and a request cannot hold the writer off.
        let verifier = Arc::clone(&*self.verifier.read());
        let outcome = verifier.verify(token);
        if let Err(AuthError::UnknownSigningKey(kid)) = &outcome {
            self.request_refresh(kid);
        }
        outcome
    }

    /// Completes when a refetch has been asked for.
    ///
    /// Awaited by the refresh task alongside its timer.
    pub async fn refresh_requested(&self) {
        self.refresh.notified().await;
    }

    fn request_refresh(&self, kid: &str) {
        let mut last = self.last_nudge.lock();
        let now = Instant::now();
        if last.is_some_and(|at| now.duration_since(at) < MIN_REFETCH_INTERVAL) {
            return;
        }
        *last = Some(now);
        debug!(kid, "a token named an unknown signing key; asking for a fresh JWKS");
        self.refresh.notify_one();
    }
}

/// The path a resource identifier's metadata is served at (RFC 9728 §3).
///
/// **Inserted between the authority and the path, not appended.** For the
/// ordinary identifier with no path this is just the well-known path, but for
/// `https://host/kimmy` it is `/.well-known/oauth-protected-resource/kimmy` —
/// which is what keeps two resources on one host distinguishable, and is the
/// same construction OIDC Discovery gets wrong often enough to be worth
/// spelling out.
fn metadata_path_for(resource: &str) -> String {
    let path = resource
        .trim_end_matches('/')
        .strip_prefix("https://")
        .and_then(|authority_and_path| {
            authority_and_path.find('/').map(|at| &authority_and_path[at..])
        })
        .unwrap_or("");
    format!("{}{path}", kimmy_auth::PROTECTED_RESOURCE_METADATA_PATH)
}

/// The absolute URL of that document, as a challenge has to name it.
fn metadata_url_for(resource: &str) -> String {
    let trimmed = resource.trim_end_matches('/');
    let authority_end = trimmed
        .strip_prefix("https://")
        .and_then(|authority_and_path| authority_and_path.find('/'))
        .map_or(trimmed.len(), |at| "https://".len() + at);
    format!("{}{}", &trimmed[..authority_end], metadata_path_for(resource))
}

#[cfg(test)]
mod tests {
    use kimmy_auth::{Action, Grant, OidcSettings, RoleMapping};

    use super::*;

    fn settings() -> OidcSettings {
        OidcSettings {
            issuer: "https://auth.example.com".into(),
            audience: "kimmydb".into(),
            roles_claim: "roles".into(),
            role_mappings: vec![RoleMapping {
                claim_value: "kimmydb-analyst".into(),
                role: None,
                grants: vec![Grant::new("sales", "orders*", vec![Action::Read])],
            }],
            require_at_jwt: false,
            allow_federated_admin: false,
            subject_claim: None,
        }
    }

    fn federation() -> Arc<Federation> {
        Federation::new(OidcVerifier::new(settings()).unwrap())
    }

    #[tokio::test]
    async fn an_unknown_key_id_asks_for_a_refetch() {
        // Key rotation recovery: without the nudge, a provider that rotates
        // between ticks refuses every request until the next tick.
        let federation = federation();
        let token = token_naming("some-rotated-key");

        assert!(federation.verify(&token).is_err());
        // Already raised, so the wait returns at once rather than hanging.
        tokio::time::timeout(Duration::from_secs(1), federation.refresh_requested())
            .await
            .expect("the refresher should have been woken");
    }

    #[tokio::test]
    async fn repeated_unknown_key_ids_do_not_repeat_the_refetch() {
        // The key id in a token is attacker-controlled. Without the rate limit,
        // sending nonsense key ids is a way to make this node hammer its own
        // identity provider.
        let federation = federation();
        for n in 0..50 {
            let _ = federation.verify(&token_naming(&format!("kid-{n}")));
        }

        // One permit, consumed by the first wait; the next one has nothing.
        tokio::time::timeout(Duration::from_secs(1), federation.refresh_requested())
            .await
            .expect("the first one is let through");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), federation.refresh_requested())
                .await
                .is_err(),
            "the other forty-nine must not each become a request to the provider"
        );
    }

    #[test]
    fn installing_a_key_set_keeps_the_settings() {
        // The swap replaces key material, never who is trusted or what a claim
        // is worth — a refetch that could change the issuer would make the
        // provider the authority on its own trustworthiness.
        let federation = federation();
        assert_eq!(federation.key_count(), 0);

        federation.install_keys(JwkSet { keys: Vec::new() });
        assert_eq!(federation.issuer(), "https://auth.example.com");
    }

    /// A syntactically valid RS256 token naming a key id, signed with nothing
    /// that verifies. Enough to reach the key lookup, which is the subject.
    fn token_naming(kid: &str) -> String {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(format!(r#"{{"alg":"RS256","typ":"JWT","kid":"{kid}"}}"#));
        let claims = b64.encode(
            r#"{"sub":"ada","iss":"https://auth.example.com","aud":"kimmydb","exp":9999999999}"#,
        );
        format!("{header}.{claims}.c2lnbmF0dXJl")
    }
}
