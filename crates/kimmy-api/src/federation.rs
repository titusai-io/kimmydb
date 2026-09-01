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
use tracing::{debug, warn};

/// Shortest gap between two key-id-driven refetches.
///
/// The bound on what an unknown `kid` can cost: whatever a caller sends, this
/// node asks its provider at most twice a minute on top of the timer. Not
/// configurable, because the number that matters is "not once per request" and
/// any value with that property is as good as any other.
pub const MIN_REFETCH_INTERVAL: Duration = Duration::from_secs(30);

/// Shortest gap between two lifetime-refusal warnings.
///
/// A lifetime refusal is not a one-off: a provider minting tokens over the
/// limit produces one on *every* request from *every* caller until somebody
/// changes a number, so an unlimited warning would be the loudest line in the
/// log precisely when the node is otherwise silent. One a minute is enough to
/// find, and few enough to sit alongside a real workload's logs.
const MIN_LIFETIME_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// The current verifier, plus the channel that asks for a fresher key set.
pub struct Federation {
    verifier: RwLock<Arc<OidcVerifier>>,
    /// Raised when a token named a key id the current set does not hold.
    refresh: Notify,
    /// When the last nudge was let through, for the rate limit above.
    last_nudge: Mutex<Option<Instant>>,
    /// When the last lifetime refusal was warned about, for the rate limit
    /// above. Separate from `last_nudge`: the two say different things and one
    /// must not silence the other.
    last_lifetime_warning: Mutex<Option<Instant>>,
}

impl Federation {
    pub fn new(verifier: OidcVerifier) -> Arc<Self> {
        Arc::new(Self {
            verifier: RwLock::new(Arc::new(verifier)),
            refresh: Notify::new(),
            last_nudge: Mutex::new(None),
            last_lifetime_warning: Mutex::new(None),
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
        match &outcome {
            Err(AuthError::UnknownSigningKey(kid)) => self.request_refresh(kid),
            Err(AuthError::TokenLifetimeExceeded { max_secs, lifetime_secs }) => {
                self.warn_about_lifetime(format_args!(
                    "the provider is minting access tokens valid for {lifetime_secs}s, longer \
                     than the {max_secs}s this node accepts; every federated request is being \
                     refused. Shorten the provider's access token lifetime for this resource, \
                     or raise auth.oidc.max_token_lifetime_secs knowingly"
                ));
            }
            Err(AuthError::TokenLifetimeUnbounded { max_secs }) => {
                self.warn_about_lifetime(format_args!(
                    "the provider is minting access tokens with no iat, so their lifetime \
                     cannot be checked against the {max_secs}s this node accepts; every \
                     federated request is being refused. RFC 9068 §2.2 requires the claim and \
                     there is no setting here that waives it"
                ));
            }
            _ => {}
        }
        outcome
    }

    /// Report a lifetime refusal to the operator, at most once a minute.
    ///
    /// At WARN and not DEBUG because it is the operator's to fix and nobody
    /// else's: the client presenting the token cannot shorten it, and the
    /// challenge it gets back names the limit but not what went wrong at the
    /// provider. Without this the refusal is a 401 with nothing anywhere to say
    /// why — which is a configuration this node can see and was choosing not to
    /// mention.
    ///
    /// Nothing from the token reaches the log but its measured lifetime, which
    /// is a property of the provider's configuration rather than of the caller.
    fn warn_about_lifetime(&self, message: std::fmt::Arguments<'_>) {
        if self.take_lifetime_warning_permit() {
            warn!("{message}");
        }
    }

    /// The rate limit itself, separated so a test can drive it without needing
    /// a provider's key material to produce a genuine refusal.
    fn take_lifetime_warning_permit(&self) -> bool {
        let mut last = self.last_lifetime_warning.lock();
        let now = Instant::now();
        if last.is_some_and(|at| now.duration_since(at) < MIN_LIFETIME_WARNING_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
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
    use kimmy_auth::{Action, DEFAULT_MAX_TOKEN_LIFETIME_SECS, Grant, OidcSettings, RoleMapping};

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
            max_token_lifetime_secs: DEFAULT_MAX_TOKEN_LIFETIME_SECS,
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
    fn a_flood_of_lifetime_refusals_is_one_warning() {
        // A provider over the limit refuses *every* request from *every*
        // caller, so the warning has to be rate-limited or it becomes the whole
        // log. One line is what makes the misconfiguration findable; the next
        // thousand only make it expensive.
        let federation = federation();
        assert!(federation.take_lifetime_warning_permit(), "the first one has to be logged");
        for _ in 0..1_000 {
            assert!(
                !federation.take_lifetime_warning_permit(),
                "a refusal inside the interval must not repeat the line"
            );
        }
    }

    #[test]
    fn the_lifetime_warning_and_the_refetch_nudge_do_not_silence_each_other() {
        // Two rate limits on two unrelated conditions. Sharing one instant
        // would make an unknown key id hide a lifetime misconfiguration, and
        // the point of the warning is that it appears when nothing else does.
        let federation = federation();
        for n in 0..10 {
            let _ = federation.verify(&token_naming(&format!("kid-{n}")));
        }
        assert!(
            federation.take_lifetime_warning_permit(),
            "the nudge limiter must not have consumed the warning's permit"
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
