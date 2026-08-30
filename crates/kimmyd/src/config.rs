//! Server configuration.
//!
//! Three sources, lowest precedence first: built-in defaults, a TOML file, then
//! CLI flags (each of which also reads a `KIMMY_*` environment variable via
//! clap). Flags win because they are the most specific thing the operator
//! typed; the file wins over defaults for the same reason.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use kimmy_cluster::SeedSource;
use serde::{Deserialize, Serialize};

/// `Eq` deliberately absent, and only from this type.
///
/// [`TelemetryConfig::sample_ratio`] is an `f64`, which is `PartialEq` and not
/// `Eq` — a ratio is a fraction, and rounding it to something comparable would
/// be inventing precision the operator did not ask for. Every other section
/// keeps `Eq`; nothing in the workspace needs a `Config` as a map key or in a
/// set, so the loss costs nothing.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub cluster: ClusterConfig,
    pub webhooks: WebhookConfig,
    pub audit: AuditConfig,
    pub log: LogConfig,
    pub telemetry: TelemetryConfig,
    pub vector: VectorConfig,
}

/// Automatic-embedding settings.
///
/// The worker itself is configured per collection (the vector configuration
/// names a provider and lives in the collection metadata); this section
/// governs the *worker* — the oplog consumer that turns collection changes
/// into provider calls on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct VectorConfig {
    /// Run the embedding worker on this node.
    ///
    /// On by default: every node consumes the oplog and keeps its own shadow
    /// collections current. Turning it off makes this node a consumer of
    /// embeddings rather than a producer of provider calls — vectors arrive
    /// by replication from whichever nodes do run workers.
    ///
    /// The intended use is cost control, not correctness. With one worker
    /// node the cluster's provider calls are exactly 1× the document count;
    /// with three they are up to 3× during replication lag, because each
    /// node's deferred re-check can fire before the owner's vectors have
    /// replicated. A deployment against a metered or CPU-bound provider can
    /// therefore run the worker on one designated member and set this to
    /// false everywhere else — the same shape the TTL sweeper's single-owner
    /// assignment gives for expiry, but chosen by the operator rather than
    /// derived.
    ///
    /// Off does **not** stop this node serving vector or hybrid search; it
    /// only stops producing new embeddings here. Search reads whatever has
    /// replicated.
    ///
    /// Prefer `--disable-vector-worker` / `KIMMY_DISABLE_VECTOR_WORKER` for
    /// the same effect from a flag, and see ADR-075 for why ownership is not
    /// derived automatically in every case.
    pub worker_enabled: bool,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self { worker_enabled: true }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Address the HTTP/WebSocket/MCP listener binds to.
    pub bind: SocketAddr,
    /// Serve the MCP endpoint at `/mcp`.
    ///
    /// On by default. It is not a privilege escalation — every tool call runs
    /// through the same authorization as the REST routes — so the toggle exists
    /// for operators who want the surface area gone, not because leaving it on
    /// grants anything a token did not already have.
    pub mcp: bool,
    /// `Host` values the MCP endpoint will accept. Empty means accept any.
    ///
    /// This is DNS-rebinding protection, and it is off by default because the
    /// attack it stops does not apply here: `/mcp` requires a bearer token,
    /// checked before the MCP transport runs, and a rebinding attack cannot
    /// forge one. Set it if you want defence in depth — but set it to every
    /// name clients actually use, or they will be refused.
    pub mcp_allowed_hosts: Vec<String>,
    pub rate_limit: RateLimitConfig,
    pub tls: TlsConfig,
    /// The URL clients should use to reach *this* node, published to the
    /// cluster so `/v1/topology` can hand it to them.
    ///
    /// Cannot be inferred, which is why it is configuration rather than
    /// something the node works out: a node bound to `0.0.0.0` has no single
    /// address, and the address clients actually reach may belong to a proxy,
    /// a load balancer or a Kubernetes service rather than to this process. A
    /// wrong guess here is published to every client in the cluster, so a node
    /// with a wildcard bind and no value set advertises nothing and says so.
    ///
    /// With a concrete bind it defaults to that address, with the scheme
    /// following whether this node terminates TLS.
    pub advertise: Option<String>,
}

/// Native TLS termination for the HTTP, WebSocket and MCP listener.
///
/// There is no `enabled` flag. TLS is on when both a certificate and a key are
/// configured and off when neither is — a separate toggle would add a state
/// where `enabled = true` with no certificate, which can only ever be a startup
/// failure. Naming exactly one of the two is refused for the same reason: it is
/// unambiguously a mistake, and the useful moment to say so is at startup.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    /// PEM certificate chain. Leaf first, then any intermediates — a client
    /// that cannot build a path to a root it trusts will refuse the connection
    /// even though the leaf itself is valid.
    pub cert_file: Option<PathBuf>,
    /// PEM private key: PKCS#8, PKCS#1 or SEC1.
    pub key_file: Option<PathBuf>,
}

impl TlsConfig {
    /// Both halves, or neither.
    pub fn pair(&self) -> Option<(&Path, &Path)> {
        match (&self.cert_file, &self.key_file) {
            (Some(cert), Some(key)) => Some((cert.as_path(), key.as_path())),
            _ => None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.pair().is_some()
    }

    fn validate(&self) -> Result<()> {
        match (&self.cert_file, &self.key_file) {
            (Some(_), None) => anyhow::bail!(
                "server.tls.cert_file is set but server.tls.key_file is not; TLS needs both, \
                 and starting without it would serve plaintext on a port an operator believes \
                 is encrypted"
            ),
            (None, Some(_)) => anyhow::bail!(
                "server.tls.key_file is set but server.tls.cert_file is not; TLS needs both, \
                 and starting without it would serve plaintext on a port an operator believes \
                 is encrypted"
            ),
            // Existence is checked here rather than at first connection: a
            // missing file should stop the node, not become a handshake failure
            // for whoever connects first.
            (Some(cert), Some(key)) => {
                for (label, path) in [("cert_file", cert), ("key_file", key)] {
                    if !path.is_file() {
                        anyhow::bail!(
                            "server.tls.{label} points at {}, which is not a readable file",
                            path.display()
                        );
                    }
                }
                Ok(())
            }
            (None, None) => Ok(()),
        }
    }
}

/// Request rate limiting.
///
/// Only `/v1/auth/login` is limited today, because that is the one route where
/// a limit is a *security* control rather than a capacity control: it is
/// unauthenticated by necessity, passwords are guessable at network speed, and
/// every attempt runs a full Argon2id verification whether or not the user
/// exists. Capacity limits on the authenticated routes want measurements behind
/// them, which is what M5's benchmarks are for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RateLimitConfig {
    /// Failed logins allowed per client address per window. Zero disables it.
    pub login_per_ip: u32,
    pub login_per_ip_window_secs: u64,
    /// Failed logins allowed per attempted username per window, across every
    /// address. Zero disables it, **which is the default**.
    ///
    /// This is the only defence against a brute force spread across many source
    /// addresses. It is off by default because it introduces a lockout: anyone
    /// who can reach the endpoint can spend a named user's budget and keep the
    /// legitimate holder out for the rest of the window. Turning it on trades a
    /// remote-guessing risk for a denial-of-service one, and which of those
    /// matters more depends on a deployment rather than on a default.
    pub login_per_user: u32,
    pub login_per_user_window_secs: u64,
    /// Header naming the real client, for a server behind a proxy. Empty means
    /// use the socket peer address.
    ///
    /// Opt-in because a forwarded header is client-supplied: trusting one by
    /// default would let any caller defeat per-address limiting by varying a
    /// header, which is worse than no limiter, because it would look like one
    /// was working. Set it only when a proxy you control rewrites the header.
    pub trusted_proxy_header: Option<String>,
    /// Upper bound on distinct keys held in memory.
    ///
    /// The key space is attacker-controlled — an address is whatever packets
    /// arrive from — so this is what keeps the defence from becoming a denial
    /// of service itself. Buckets that have refilled are dropped first.
    pub max_tracked_keys: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Directory holding the redb file, node identity, and model cache.
    pub data_dir: PathBuf,
    /// How long deleted documents are retained as tombstones.
    ///
    /// This must exceed the longest network partition you are willing to
    /// tolerate. If a partitioned peer rejoins after its tombstones have been
    /// collected here, documents it deleted will resurrect.
    pub tombstone_retention_secs: u64,
    /// How much oplog history to keep for change-stream resumption and peer
    /// catch-up. A subscriber that lags past this gets an `invalidate`.
    pub oplog_retention_secs: u64,
    /// Bound on the storage engine's page cache, in bytes.
    ///
    /// This is most of a node's resident memory. redb keeps up to this many
    /// bytes of database pages and evicts only when it needs the room — never
    /// on a timer — so a node settles at whatever its busiest period filled
    /// and stays there. 256 MiB by default: comfortably more than the working
    /// set of the deployments this project serves today, a quarter of redb's
    /// own 1 GiB default, and the number to raise on a node whose database
    /// file is much larger than that and whose reads are latency-sensitive.
    pub cache_bytes: u64,
    /// How often to collect records that are past their retention.
    ///
    /// Separate from the retention windows themselves: retention says what is
    /// garbage, this says how often to look. Zero disables collection, which
    /// restores the pre-M5 behaviour of unbounded growth — available because an
    /// operator debugging a replication problem may want the history kept.
    pub gc_interval_secs: u64,
    /// How often to look for documents a TTL index says have expired.
    ///
    /// Separate from `gc_interval_secs`: that collects *garbage* — oplog
    /// entries and tombstones past their retention — while this deletes *live
    /// documents* a policy says are due. Conflating them would tie how promptly
    /// a session expires to how often disk is reclaimed.
    ///
    /// Zero disables expiry, which leaves any TTL index defined but inert.
    pub ttl_interval_secs: u64,
    /// Documents a `multi: true` update or delete commits per transaction
    /// (ADR-086).
    ///
    /// The writer is released between chunks, so this bounds how long one
    /// request can hold it; a crash or a refusal loses at most the chunk in
    /// flight. Must be between 1 and 10,000 — the same ceiling
    /// `find_and_modify` holds the writer for.
    pub multi_chunk_docs: usize,
    /// How a commit becomes durable (ADR-088): `durable` (every commit
    /// fsyncs before it returns; the default) or `coalesced` (a commit
    /// waits for the next shared fsync, one per `commit_coalesce_ms`
    /// window, so concurrent writers share it). Both are durable when the
    /// response returns; there is deliberately no class that is not.
    pub durability: String,
    /// The coalescing window, in milliseconds, for `durability = "coalesced"`.
    /// Ignored under `durable`. 1 to 1000.
    pub commit_coalesce_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Disable authentication entirely. Refuses to combine with a non-loopback
    /// bind address — see [`Config::validate`].
    pub insecure_no_auth: bool,
    /// Bootstrap superuser, created on first start only.
    pub root_user: String,
    /// Bootstrap password. Prefer the `KIMMY_ROOT_PASSWORD` env var over
    /// writing this into a file.
    #[serde(serialize_with = "redact")]
    pub root_password: Option<String>,
    /// Shared secret for signing JWTs. Every node in a cluster must agree, or
    /// tokens issued by one node will be rejected by another.
    #[serde(serialize_with = "redact")]
    pub jwt_secret: Option<String>,
    pub token_ttl_secs: u64,
    /// Where the password login answers (ADR-100).
    pub local: LocalConfig,
    /// Federation with an external OpenID Connect provider.
    pub oidc: OidcConfig,
}

/// The local, password-and-user-store half of authentication.
///
/// A section of its own rather than a key on `[auth]` so that what is being
/// configured is named: `auth.local.login` reads as "where the *local* login
/// answers", beside `auth.oidc.*` for the other way in. Local tokens are
/// verified everywhere regardless of anything here — this section is about
/// minting them, and the field documentation says so.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LocalConfig {
    /// Where `POST /v1/auth/login` (and `/v1/auth/refresh`, which mints too)
    /// answers: `always`, `loopback_only` or `disabled`.
    ///
    /// `loopback_only` judges the **TCP peer** of the connection and ignores
    /// any forwarded header, so a reverse proxy on the same host makes every
    /// caller look local. `disabled` is refused unless `auth.oidc` is
    /// configured — see [`Config::validate`]. A token already issued keeps
    /// verifying under every mode; this governs minting, not verifying.
    pub login: String,
}

impl Default for LocalConfig {
    fn default() -> Self {
        // What shipped: the route answers everyone. Anything stricter is a
        // choice an operator makes knowing where root will log in from.
        Self { login: kimmy_api::LocalLogin::Always.name().to_string() }
    }
}

/// What a secret serializes as, in place of itself.
///
/// Below the 16 bytes [`Config::validate`] requires of a signing key, on
/// purpose: `check-config` output pasted back into a config file has to fail at
/// startup rather than run with a placeholder for a key. That refusal comes
/// first whenever authentication is on, which is also what stops the bootstrap
/// password below from being taken literally on a fresh database.
const REDACTED: &str = "<redacted>";

/// Serialize a secret as [`REDACTED`], keeping only whether it is set.
///
/// The only thing that serializes a [`Config`] is `check-config`, whose whole
/// job is to be read by a person — in a terminal, in CI output, pasted into a
/// bug report. The value of `jwt_secret` in particular signs every local token
/// this cluster issues, so anyone who reads it can mint a principal, `root`
/// included; and the config file deliberately keeps both of these commented out
/// in favour of `KIMMY_JWT_SECRET` and `KIMMY_ROOT_PASSWORD`, so the documented
/// workflow is precisely the one that used to print them.
///
/// `None` still serializes as absent. "Is it set?" is the question
/// `check-config` exists to answer and is not itself a secret.
fn redact<S: serde::Serializer>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(_) => serializer.serialize_some(REDACTED),
        None => serializer.serialize_none(),
    }
}

/// Trust in one external identity provider.
///
/// There is no `enabled` flag, for the same reason [`TlsConfig`] has none:
/// a toggle would add a state where `enabled = true` with nothing configured,
/// which can only ever be a startup failure. Federation is on when the section
/// says anything at all, and a half-filled section is refused — naming an
/// issuer without an audience is unambiguously a mistake, and the useful
/// moment to say so is at startup rather than the first time a token arrives.
///
/// Local users keep working alongside it. This adds a second verifier; it does
/// not replace the first (ADR-064).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OidcConfig {
    /// The provider's issuer URL, matched against a token's `iss` exactly.
    ///
    /// Also where discovery starts: `{issuer}/.well-known/openid-configuration`
    /// names the JWKS this node fetches signing keys from.
    ///
    /// One issuer, not a list. A second one is a second trust root, and
    /// "which of these may say a subject is an analyst" is a question this
    /// round does not answer (ADR-064).
    pub issuer: Option<String>,
    /// The `aud` a token must carry to be accepted here.
    ///
    /// Required, because a provider signs tokens for every application that
    /// trusts it. Without an audience restriction, a token minted for the
    /// company wiki would authenticate against this database.
    pub audience: Option<String>,
    /// Claim carrying the caller's roles. `groups` for Entra ID.
    pub roles_claim: String,
    /// Claim values, and the grants each is worth here.
    ///
    /// Written inline rather than stored in a table this database owns
    /// (ADR-066): the mapping is configuration an operator reviews and diffs,
    /// so changing it is a restart, and a node cannot disagree with its file
    /// about who may do what.
    pub role_mappings: Vec<kimmy_auth::RoleMapping>,
    /// How often to re-fetch the provider's signing keys.
    ///
    /// A rotation between two ticks is covered separately — a token naming an
    /// unknown key id triggers one rate-limited refetch — so this interval is
    /// about staying current, not about how fast a rotation is survived.
    pub refresh_interval_secs: u64,
    /// Require `typ: at+jwt` on a federated token (RFC 9068 §4).
    ///
    /// **Off by default, deliberately.** The header exists so an access token
    /// cannot be mistaken for an ID token, but providers disagree about
    /// stamping it — Entra ID sends `typ: JWT` on v2 access tokens — so a
    /// strict default would refuse every token from a provider this
    /// federation is meant to support.
    ///
    /// Turn it on when your provider is known to emit it. If the audience is
    /// an https URL then ADR-071 has already closed the confusion this
    /// guards against, because an ID token's audience is a client id and can
    /// never be this node's resource identifier; the setting is then defence
    /// in depth. With an opaque audience it is the real check.
    pub require_at_jwt: bool,
    /// Let a federated principal hold the `admin` action (ADR-074).
    ///
    /// **Off by default, which is exactly the behaviour that shipped.**
    /// ADR-067 reserved `admin` to local users so a misconfigured or
    /// compromised identity provider could not mint a superuser over this
    /// database. That is still the right default, and it was tested rather than
    /// merely reasoned about: a provider asserting `roles: ["user", "admin"]`
    /// got exactly what the `user` mapping said and nothing more.
    ///
    /// The flag exists because the same rule made the enterprise deployment
    /// impossible. Large organisations run joiner-mover-leaver, and auditors
    /// specifically flag privileged local accounts living outside the IdP —
    /// which this rule *requires*. MinIO, Vault, Grafana and Elasticsearch all
    /// allow the mapping; the canonical break-glass pattern is to allow it and
    /// separately keep an emergency local account, not to forbid it.
    ///
    /// Turning it on is loud: the node says so at startup, every time.
    pub allow_federated_admin: bool,
    /// A claim carried as a federated principal's **display** name (ADR-100):
    /// `preferred_username`, `email`, `upn`.
    ///
    /// A provider's `sub` is stable and opaque — a GUID, an `00u…` string —
    /// which makes it a good identity and a bad thing to read in an audit
    /// line. This names the claim a person would recognise, and it appears in
    /// `whoami` and the audit record and nowhere else: `sub` stays the
    /// identity for authorization, role resolution, rate limiting and every
    /// comparison the server makes, because an email is mutable and not
    /// unique across providers. A token whose claim is missing or not a string
    /// is not refused; the display falls back to `sub`.
    ///
    /// `None`, the default, keeps the subject as the display name.
    pub subject_claim: Option<String>,
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            issuer: None,
            audience: None,
            subject_claim: None,
            // What most providers use. Entra ID is the notable exception.
            roles_claim: "roles".to_string(),
            role_mappings: Vec::new(),
            // Five minutes: far inside the hours a provider leaves a retiring
            // key published, and rare enough to be invisible as load.
            refresh_interval_secs: 300,
            // Off, so that a provider which does not stamp the header keeps
            // working. See the field's own documentation for why that is the
            // safe default rather than the lax one.
            require_at_jwt: false,
            // Off, so that enabling federation changes nothing about who may
            // administer this database. See the field's documentation.
            allow_federated_admin: false,
        }
    }
}

impl OidcConfig {
    /// Whether the operator asked for federation at all.
    ///
    /// Any setting in the section counts, including a half-filled one, so that
    /// an incomplete section is refused rather than silently ignored — the
    /// failure mode being avoided is a node that starts with federation
    /// quietly off and refuses every token the IdP issues.
    pub fn is_configured(&self) -> bool {
        self.issuer.is_some() || self.audience.is_some() || !self.role_mappings.is_empty()
    }

    /// The settings the verifier is built from, once validated.
    pub fn settings(&self) -> Option<kimmy_auth::OidcSettings> {
        Some(kimmy_auth::OidcSettings {
            issuer: self.issuer.clone()?,
            audience: self.audience.clone()?,
            roles_claim: self.roles_claim.clone(),
            role_mappings: self.role_mappings.clone(),
            require_at_jwt: self.require_at_jwt,
            allow_federated_admin: self.allow_federated_admin,
            subject_claim: self.subject_claim.clone(),
        })
    }

    fn validate(&self) -> Result<()> {
        if !self.is_configured() {
            return Ok(());
        }

        let Some(issuer) = self.issuer.as_deref() else {
            anyhow::bail!(
                "auth.oidc is configured but no issuer is set; there is nothing to match a \
                 token's `iss` against and nowhere to fetch signing keys from, so every \
                 federated token would be refused. Set auth.oidc.issuer (KIMMY_OIDC_ISSUER)."
            );
        };
        if self.audience.is_none() {
            anyhow::bail!(
                "auth.oidc.issuer is set but auth.oidc.audience is not; a provider signs tokens \
                 for every application that trusts it, so without an audience a token minted \
                 for an unrelated application would authenticate against this database. Set \
                 auth.oidc.audience (KIMMY_OIDC_AUDIENCE)."
            );
        }
        // Not tidiness: the discovery document and the JWKS are fetched from
        // this URL, and over plaintext anyone on the path can substitute their
        // own signing keys — which is a way to mint any principal they like.
        if !issuer.starts_with("https://") {
            anyhow::bail!(
                "auth.oidc.issuer is {issuer:?}, which is not https. The discovery document and \
                 the signing keys are fetched from it, so over plaintext anyone on the network \
                 path could substitute their own keys and mint any identity they liked."
            );
        }
        if self.roles_claim.trim().is_empty() {
            anyhow::bail!(
                "auth.oidc.roles_claim is empty; no claim would be read, so every federated \
                 caller would arrive with no grants at all. Set it to `roles`, or to `groups` \
                 for Entra ID."
            );
        }
        // Refused rather than treated as unset: an operator who wrote the key
        // meant to name a claim, and a blank one would silently mean "the
        // subject", which is what they were trying to get away from.
        if self.subject_claim.as_deref().is_some_and(|c| c.trim().is_empty()) {
            anyhow::bail!(
                "auth.oidc.subject_claim is empty; a claim with no name can never resolve. Name \
                 the claim that carries a readable identity — `preferred_username`, `email`, \
                 `upn` — or omit the setting (KIMMY_OIDC_SUBJECT_CLAIM) to show the subject."
            );
        }
        if self.refresh_interval_secs == 0 {
            anyhow::bail!(
                "auth.oidc.refresh_interval_secs must be greater than zero; a node that never \
                 re-fetches the provider's signing keys would refuse every token once the \
                 provider rotated them"
            );
        }

        // The `admin` refusal, and anything else the verifier itself would
        // reject. Checked here as well as in `OidcVerifier::new` so that
        // `check-config` refuses exactly what the server refuses — an
        // entrypoint check that blesses a configuration the node then rejects
        // is worse than no check.
        //
        // An *unknown* action never reaches this: `Grant::actions` is a typed
        // enum, so `actions = ["delet"]` fails while the file is being parsed,
        // with an error naming the bad value and listing the valid ones.
        if let Some(settings) = self.settings() {
            settings.validate()?;
        }
        Ok(())
    }

    /// One-line form for the startup summary. Never the mappings themselves —
    /// they are long, and the count is what tells an operator the file was read.
    ///
    /// `allow_federated_admin` is named only when it is on. A default that is
    /// printed every time is a default nobody reads; a line that appears only
    /// when a security boundary has been lowered is one somebody notices.
    fn describe(&self) -> String {
        match &self.issuer {
            None => "off".to_string(),
            Some(issuer) => {
                let admin =
                    if self.allow_federated_admin { ", FEDERATED ADMIN ALLOWED" } else { "" };
                // Named when set, for the same reason the admin flag is: the
                // default is silent, and a display claim that was configured
                // is a fact an operator reading an audit line wants confirmed.
                let display = match &self.subject_claim {
                    Some(claim) => format!(", display from {claim}"),
                    None => String::new(),
                };
                format!("{issuer} ({} role mappings{display}{admin})", self.role_mappings.len())
            }
        }
    }
}

impl LocalConfig {
    /// The mode, once the name has been checked.
    pub fn login_mode(&self) -> Result<kimmy_api::LocalLogin> {
        kimmy_api::LocalLogin::parse(&self.login).map_err(|e| anyhow::anyhow!("auth.local.{e}"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    pub enabled: bool,
    /// Address the cluster replication transport binds to (TCP).
    pub bind: SocketAddr,
    /// Where to look for peers. Re-resolved periodically, so a Kubernetes
    /// headless service picks up new pods without a restart.
    pub seeds: Vec<SeedSource>,
    /// Shared secret authenticating node-to-node traffic.
    pub cluster_secret: Option<String>,
    /// How often to run an anti-entropy round against each known peer.
    pub sync_interval_secs: u64,
    /// How often to re-resolve the seed sources.
    ///
    /// Slower than syncing on purpose: DNS is the expensive half, and a pod set
    /// does not change every few seconds. But it must repeat — a node that
    /// resolved only at startup would never see a peer that joined later.
    pub discovery_interval_secs: u64,
    /// Gossip membership over UDP, so the cluster agrees who is alive.
    ///
    /// On by default. With it off, peers come from discovery alone and each
    /// node forms its own private opinion of liveness from failed connections —
    /// workable, but two nodes can then disagree about a third indefinitely.
    pub membership: bool,
    /// Peers contacted per round.
    ///
    /// A cap, not a quota: a cluster smaller than this contacts everyone.
    /// Keeping it constant is what makes the per-round cost independent of
    /// cluster size — anti-entropy is transitive, so a write still reaches
    /// everyone through intermediate peers.
    pub fanout: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable, for a terminal.
    Pretty,
    /// One JSON object per line, for log shippers.
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WebhookConfig {
    /// Hosts a webhook may target beyond the public internet.
    ///
    /// Empty by default, which means public addresses only. Loopback,
    /// link-local (169.254.0.0/16 — cloud metadata) and RFC1918 ranges are
    /// refused unless the host is named here, because otherwise anyone who can
    /// register a webhook can make this node probe its own network.
    ///
    /// Naming a host exempts it from the address checks entirely, so add only
    /// the ones you mean.
    pub allowed_hosts: Vec<String>,

    /// How many deliveries this node may have in flight at once.
    ///
    /// A cap rather than "as many as there are subscriptions": a webhook on a
    /// hot collection would otherwise be free to consume every outbound
    /// connection the node has. Bounded concurrency is also what stops one
    /// endpoint that has stopped answering from delaying every subscription
    /// behind it, which is what a serial dispatcher does.
    pub max_concurrent_deliveries: usize,

    /// The largest request body a delivery may carry.
    ///
    /// Batches are trimmed to fit. A *single* event whose document already
    /// exceeds this is delivered with `fullDocument` omitted rather than
    /// dropped — the receiver still learns the change happened and can fetch
    /// the document itself. Skipping it would leave a gap the receiver could
    /// never detect.
    pub max_payload_bytes: usize,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            max_concurrent_deliveries: 8,
            max_payload_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuditConfig {
    /// What authorization decisions to record: `off`, `denials`, `writes`,
    /// or `all`.
    ///
    /// `denials` by default. `all` writes one line per authorized operation,
    /// which on a read-heavy node is one per request — a real cost, and the
    /// reason it is not the default. A denial is rare and is the event someone
    /// is actually watching for.
    ///
    /// Records go to the `kimmy::audit` tracing target, so they can be routed
    /// separately with a filter directive.
    pub mode: String,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self { mode: "denials".to_string() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// A `tracing-subscriber` env-filter directive, e.g. `info,kimmy_storage=debug`.
    pub level: String,
    pub format: LogFormat,
}

/// Where traces and metrics are exported, if anywhere.
///
/// There is no `enabled` flag, for the same reason [`TlsConfig`] and
/// [`OidcConfig`] have none: a toggle would add a state where `enabled = true`
/// with no endpoint, which can only ever be a startup failure. Telemetry is on
/// when `endpoint` is set and off when it is not, and the settings beside it
/// are inert until it is — so a half-filled section is a section nobody has
/// finished rather than a mistake.
///
/// **HTTP only, never gRPC** (ADR-069). The exporter rides the `reqwest` and
/// `rustls`/`ring` stack the build already carries, which is what keeps
/// `scripts/check-native-deps.sh` unchanged and the musl and arm64
/// cross-compiles exactly as hard as they were.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TelemetryConfig {
    /// Base URL of an OpenTelemetry collector. `None` — the default — means no
    /// exporter is built and no span leaves this process.
    ///
    /// The **base**, not a signal path: `/v1/traces` and `/v1/metrics` are
    /// appended, matching what `OTEL_EXPORTER_OTLP_ENDPOINT` means everywhere
    /// else, so an operator can paste the same value they gave their other
    /// services.
    pub endpoint: Option<String>,
    /// `http/protobuf` (the default) or `http/json`.
    ///
    /// Protobuf because it is what a collector's :4318 receiver expects and is
    /// the cheaper encoding; JSON exists because it is the one an operator can
    /// read off a packet capture when a receiver is rejecting exports and
    /// nothing says why.
    pub protocol: String,
    /// Fraction of traces to record, `0.0` to `1.0`.
    ///
    /// Applied as a parent-based sampler, so a request that arrives already
    /// carrying a sampled `traceparent` is recorded whatever this says. That is
    /// the property that makes a trace whole: a ratio applied independently per
    /// node produces traces with holes in them, which is worse than fewer
    /// complete ones.
    pub sample_ratio: f64,
    /// Whether spans may carry database and collection names.
    ///
    /// **Off by default**, and the reason [`kimmy_api::telemetry`] exists.
    /// Span names come from `http.route` and `db.operation.name`, neither of
    /// which names anything a deployment stores; `url.path`, `db.namespace`
    /// and `db.collection.name` do, so they are exported only when this says
    /// so. See ADR-068 and docs/security.md.
    pub include_names: bool,
    /// What `service.name` this node reports itself as.
    ///
    /// One name for the whole deployment, not one per node — the node is
    /// distinguished by `service.instance.id`, and a per-node service name
    /// makes a three-node cluster look like three unrelated systems.
    pub service_name: String,
    /// How long one export may take before it is abandoned.
    pub export_timeout_secs: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            // Off. Everything below is inert until this is set.
            endpoint: None,
            protocol: "http/protobuf".to_string(),
            // Everything. A database is not a service where a hundredth of the
            // requests is a useful sample of an incident, and an operator who
            // needs less can say so — whereas one who discovers after the fact
            // that the trace was dropped has nothing to look at.
            sample_ratio: 1.0,
            include_names: false,
            service_name: "kimmydb".to_string(),
            // Longer than the collector should ever take, short enough that a
            // black-holed endpoint does not park an exporter thread for a
            // minute. Exports are off the request path either way.
            export_timeout_secs: 10,
        }
    }
}

impl TelemetryConfig {
    /// Whether an exporter should be built at all.
    pub fn is_configured(&self) -> bool {
        self.endpoint.is_some()
    }

    fn validate(&self) -> Result<()> {
        let Some(endpoint) = self.endpoint.as_deref() else {
            return Ok(());
        };

        // An absolute URL, because it is a base the signal path is appended to
        // — `localhost:4318` parses as a scheme-relative something and would be
        // requested as a path on nothing.
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            anyhow::bail!(
                "telemetry.endpoint is {endpoint:?}, which is not an absolute http:// or \
                 https:// URL. It is the base a signal path is appended to, so a bare host and \
                 port has nowhere to send anything. Set it to e.g. \
                 http://otel-collector:4318 (KIMMY_OTLP_ENDPOINT)."
            );
        }
        // Refused rather than left to fail at the first export. The exporter is
        // built without a TLS backend — ADR-069 keeps the second TLS stack that
        // would need out of this build — so an https endpoint would produce a
        // node that serves perfectly while every export fails into a log
        // nobody reads, which looks exactly like a collector nobody configured.
        if endpoint.starts_with("https://") {
            anyhow::bail!(
                "telemetry.endpoint is {endpoint:?}, and the OTLP exporter is built without a \
                 TLS backend, so every export would fail while the node kept serving — a \
                 collector that silently receives nothing. Point it at the collector's \
                 plaintext receiver (:4318 by default), or run a collector alongside this node \
                 and let that forward over TLS. See ADR-069."
            );
        }

        if !matches!(self.protocol.as_str(), "http/protobuf" | "http/json") {
            anyhow::bail!(
                "telemetry.protocol is {:?}; expected \"http/protobuf\" or \"http/json\" \
                 (KIMMY_OTLP_PROTOCOL). gRPC is deliberately not built in — ADR-069 — because \
                 it would add tonic and its build machinery for a wire format the collector \
                 already accepts over HTTP.",
                self.protocol
            );
        }

        if !self.sample_ratio.is_finite() || !(0.0..=1.0).contains(&self.sample_ratio) {
            anyhow::bail!(
                "telemetry.sample_ratio is {}; it is a fraction of traces to record and must be \
                 between 0.0 and 1.0 (KIMMY_OTLP_SAMPLE_RATIO)",
                self.sample_ratio
            );
        }
        // Not a way to turn telemetry off. It configures an exporter, a batch
        // processor and a connection to a collector that can never emit a
        // single span — and the operator would find that out from an empty
        // dashboard weeks later. The way to turn telemetry off is to leave
        // `endpoint` unset, and the useful moment to say so is now, while
        // somebody is watching the boot.
        if self.sample_ratio == 0.0 {
            anyhow::bail!(
                "telemetry.endpoint is set but telemetry.sample_ratio is 0.0, which builds an \
                 exporter that can never emit a span. To turn telemetry off, remove \
                 telemetry.endpoint; to record a fraction of traces, set a ratio above zero."
            );
        }

        if self.export_timeout_secs == 0 {
            anyhow::bail!(
                "telemetry.export_timeout_secs must be greater than zero; an export allowed no \
                 time at all would time out before it was sent, so nothing would ever reach the \
                 collector"
            );
        }

        if self.service_name.trim().is_empty() {
            anyhow::bail!(
                "telemetry.service_name is empty; every span this node exports would arrive \
                 under a blank service and be impossible to tell apart from anything else \
                 reporting to the same collector (KIMMY_OTLP_SERVICE_NAME)"
            );
        }

        Ok(())
    }

    /// One-line form for the startup summary.
    ///
    /// The endpoint and the two settings that change what is *sent*. Naming
    /// `include_names` here is the point: it is the difference between a
    /// collector holding operation names and one holding a deployment's
    /// schema, and an operator should be able to see which they turned on
    /// without reading the file back.
    fn describe(&self) -> String {
        match &self.endpoint {
            None => "off".to_string(),
            Some(endpoint) => format!(
                "{endpoint} ({}, ratio={}, names={})",
                self.protocol,
                self.sample_ratio,
                if self.include_names { "on" } else { "off" },
            ),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:7878".parse().expect("valid literal"),
            mcp: true,
            mcp_allowed_hosts: Vec::new(),
            rate_limit: RateLimitConfig::default(),
            tls: TlsConfig::default(),
            advertise: None,
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            // Ten failures a minute is far below what guessing a password
            // needs, and far above what any legitimate client produces —
            // only failures count, so a correct client never spends any.
            login_per_ip: 10,
            login_per_ip_window_secs: 60,
            // Off. See the field documentation: enabling it is a trade, not
            // an improvement.
            login_per_user: 0,
            login_per_user_window_secs: 300,
            trusted_proxy_header: None,
            // ~100k keys of a two-field bucket plus a short string key is a
            // few megabytes — cheap enough not to need tuning, small enough
            // to bound.
            max_tracked_keys: 100_000,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/kimmy"),
            tombstone_retention_secs: 24 * 60 * 60,
            oplog_retention_secs: 24 * 60 * 60,
            cache_bytes: 256 * 1024 * 1024,
            // Frequent enough that disk use tracks retention rather than
            // sawtoothing, rare enough that the scan is not a background load.
            gc_interval_secs: 10 * 60,
            // Sixty seconds, as MongoDB uses. A TTL is a retention policy
            // rather than a deadline, so a tighter interval would multiply
            // scans for accuracy no caller can observe.
            ttl_interval_secs: 60,
            multi_chunk_docs: kimmy_storage::modify::DEFAULT_MULTI_CHUNK_DOCS,
            durability: "durable".into(),
            // A few milliseconds: long enough that concurrent writers land
            // in the same window, short enough to be invisible next to the
            // fsync it replaces.
            commit_coalesce_ms: 5,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            insecure_no_auth: false,
            root_user: "root".to_string(),
            root_password: None,
            jwt_secret: None,
            token_ttl_secs: 60 * 60,
            local: LocalConfig::default(),
            oidc: OidcConfig::default(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: format!("0.0.0.0:{}", kimmy_cluster::DEFAULT_CLUSTER_PORT)
                .parse()
                .expect("valid literal"),
            seeds: Vec::new(),
            cluster_secret: None,
            sync_interval_secs: 5,
            discovery_interval_secs: 30,
            membership: true,
            fanout: kimmy_cluster::DEFAULT_FANOUT,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: "info".to_string(), format: LogFormat::Pretty }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing config file {}", path.display()))
    }

    /// Reject combinations that would be unsafe or simply not work, at startup
    /// rather than at first request.
    pub fn validate(&self) -> Result<()> {
        if self.auth.insecure_no_auth && !is_loopback(&self.server.bind) {
            anyhow::bail!(
                "auth.insecure_no_auth is set but the server binds to {}, which is reachable \
                 from the network. Bind to 127.0.0.1 or configure authentication.",
                self.server.bind
            );
        }

        if !self.auth.insecure_no_auth && self.auth.root_password.is_none() {
            anyhow::bail!(
                "no root password configured. Set KIMMY_ROOT_PASSWORD (preferred), set \
                 auth.root_password in the config file, or pass --insecure-no-auth to run \
                 without authentication on loopback."
            );
        }

        // A node that verifies tokens must be given the key it signs them with.
        // Without one the server would otherwise fall back to a constant that
        // ships in the source, so anyone could forge a root token and every
        // deployment would share the same signing secret — a single-node hole,
        // not just a cluster one, which is why this is checked whenever auth is
        // on rather than only under `cluster.enabled`. In a cluster the same key
        // must be set identically on every node, or a token issued by one is
        // rejected by the next; catching it here avoids an intermittent 401 that
        // only shows up when a request lands on the "wrong" node.
        if !self.auth.insecure_no_auth {
            let min = kimmy_auth::MIN_SECRET_LEN;
            match &self.auth.jwt_secret {
                None => anyhow::bail!(
                    "authentication is enabled but no auth.jwt_secret is configured; the server \
                     would sign tokens with a constant compiled into the binary, so anyone \
                     could forge a token. Set KIMMY_JWT_SECRET ({min} bytes or more), and set \
                     it identically on every node of a cluster."
                ),
                // Checked here as well as in `TokenIssuer::new` so that
                // `check-config` gives the same answer the server would. An
                // entrypoint check that blesses a configuration the node then
                // refuses is worse than no check.
                Some(secret) if secret.len() < min => anyhow::bail!(
                    "auth.jwt_secret is {} bytes; {min} or more are required, because the whole \
                     cluster shares this one value and a short one makes offline brute force \
                     cheap.",
                    secret.len()
                ),
                Some(_) => {}
            }
        }

        // Federation is refused outright with authentication off rather than
        // quietly ignored: `--insecure-no-auth` makes every request a
        // superuser, so a node holding both would be handing out more than the
        // role mappings say while looking like it enforced them.
        if self.auth.insecure_no_auth && self.auth.oidc.is_configured() {
            anyhow::bail!(
                "auth.oidc is configured together with auth.insecure_no_auth; with \
                 authentication disabled every request already runs as a superuser, so the \
                 role mappings would be enforcing nothing. Remove one of the two."
            );
        }
        self.auth.oidc.validate()?;

        // Parsed here so a typo is a boot failure rather than the most
        // permissive mode by accident, and checked against federation so that
        // `disabled` can never leave a node nobody can log in to (ADR-100).
        // The mode says nothing about verifying — a token already issued
        // keeps working — so nothing here needs to consider existing sessions.
        let local_login = self.auth.local.login_mode()?;
        if local_login == kimmy_api::LocalLogin::Disabled && !self.auth.oidc.is_configured() {
            anyhow::bail!(
                "auth.local.login is \"disabled\" but auth.oidc is not configured, so nobody \
                 could ever authenticate: the password route would answer 404 and there is no \
                 identity provider to answer instead. Configure auth.oidc, or use \
                 \"loopback_only\" (KIMMY_LOCAL_LOGIN) to keep the login reachable from this \
                 host alone."
            );
        }

        if self.cluster.enabled {
            if self.cluster.seeds.is_empty() {
                anyhow::bail!(
                    "cluster.enabled is set but no seeds are configured; a node with no \
                     discovery source can never find peers. Set --seeds, e.g. \
                     --seeds k8s:kimmy-headless.default.svc.cluster.local"
                );
            }
            if self.cluster.cluster_secret.is_none() {
                anyhow::bail!(
                    "cluster.enabled is set but no cluster_secret is configured; peers would \
                     accept replication traffic from anyone. Set KIMMY_CLUSTER_SECRET."
                );
            }
        }

        if self.cluster.enabled && self.cluster.sync_interval_secs == 0 {
            anyhow::bail!(
                "cluster.sync_interval_secs must be greater than zero; a node that never runs \
                 an anti-entropy round would serve peers but never catch up itself"
            );
        }
        if self.cluster.enabled && self.cluster.fanout == 0 {
            anyhow::bail!(
                "cluster.fanout must be greater than zero; a node that contacts no peers per \
                 round would serve replication but never pull anything itself"
            );
        }
        if self.cluster.enabled && self.cluster.discovery_interval_secs == 0 {
            anyhow::bail!(
                "cluster.discovery_interval_secs must be greater than zero; a node that never \
                 re-resolves its seeds would never see a peer that joined after it started"
            );
        }

        if self.webhooks.max_concurrent_deliveries == 0 {
            anyhow::bail!(
                "webhooks.max_concurrent_deliveries must be greater than zero; a node that may \
                 have no delivery in flight would never deliver a webhook at all"
            );
        }
        if self.webhooks.max_payload_bytes == 0 {
            anyhow::bail!(
                "webhooks.max_payload_bytes must be greater than zero; a body that may hold no \
                 bytes leaves every delivery with nothing to carry"
            );
        }

        self.server.rate_limit.validate()?;
        self.server.tls.validate()?;
        self.telemetry.validate()?;
        // Parsed at startup so a typo is a boot failure rather than an audit
        // log that silently records nothing.
        kimmy_api::AuditMode::parse(&self.audit.mode).map_err(|e| anyhow::anyhow!("audit.{e}"))?;

        if self.storage.cache_bytes < 8 * 1024 * 1024 {
            anyhow::bail!("storage.cache_bytes must be at least 8 MiB (8388608)");
        }
        if self.storage.oplog_retention_secs == 0 {
            anyhow::bail!("storage.oplog_retention_secs must be greater than zero");
        }

        if self.storage.tombstone_retention_secs == 0 {
            anyhow::bail!(
                "storage.tombstone_retention_secs must be greater than zero; collecting a \
                 tombstone the instant it is written lets a peer that never saw the delete \
                 resurrect the document"
            );
        }

        // A tombstone collected before the oplog entry that carried the delete
        // is a delete a peer can still be *told about* but can no longer
        // *out-argue*: the peer replays the entry, finds no tombstone to lose
        // against, and its own older image of the document wins. Retention set
        // the other way round is the only configuration in which a partition
        // shorter than the oplog window resurrects data (ADR-085).
        if kimmy_storage::DurabilityClass::parse(&self.storage.durability).is_none() {
            anyhow::bail!(
                "storage.durability must be \"durable\" or \"coalesced\", got {:?}; there is \
                 deliberately no class under which an acknowledged write can be lost",
                self.storage.durability
            );
        }
        if !(1..=1000).contains(&self.storage.commit_coalesce_ms) {
            anyhow::bail!(
                "storage.commit_coalesce_ms ({}) must be between 1 and 1000",
                self.storage.commit_coalesce_ms
            );
        }

        if self.storage.multi_chunk_docs == 0
            || self.storage.multi_chunk_docs > kimmy_storage::MAX_CANDIDATES
        {
            anyhow::bail!(
                "storage.multi_chunk_docs ({}) must be between 1 and {}: zero would never \
                 advance, and more would hold the single writer for longer than any other \
                 request may",
                self.storage.multi_chunk_docs,
                kimmy_storage::MAX_CANDIDATES,
            );
        }

        if self.storage.tombstone_retention_secs < self.storage.oplog_retention_secs {
            anyhow::bail!(
                "storage.tombstone_retention_secs ({}) is shorter than \
                 storage.oplog_retention_secs ({}); a delete would be collected while the \
                 oplog still offers it to peers, and a peer that missed it could resurrect \
                 the document. Raise tombstone retention to at least the oplog window.",
                self.storage.tombstone_retention_secs,
                self.storage.oplog_retention_secs,
            );
        }

        // A collection pass rarer than the window it enforces means records
        // outlive their retention by up to a whole interval. Not unsafe, but it
        // makes `oplog_retention_secs` a number that does not mean what it says,
        // which is worse than a number that is simply large.
        if self.storage.gc_interval_secs > self.storage.oplog_retention_secs {
            anyhow::bail!(
                "storage.gc_interval_secs ({}) exceeds storage.oplog_retention_secs ({}), so \
                 entries would be retained for up to {} seconds rather than the configured \
                 window. Lower the interval, or raise the retention.",
                self.storage.gc_interval_secs,
                self.storage.oplog_retention_secs,
                self.storage.gc_interval_secs + self.storage.oplog_retention_secs,
            );
        }

        Ok(())
    }

    /// Redacted form, safe to log at startup.
    pub fn summary(&self) -> String {
        let seeds = if self.cluster.seeds.is_empty() {
            "none".to_string()
        } else {
            self.cluster.seeds.iter().map(SeedSource::describe).collect::<Vec<_>>().join(", ")
        };
        let gc = if self.storage.gc_interval_secs == 0 {
            "off".to_string()
        } else {
            format!("{}s", self.storage.gc_interval_secs)
        };
        format!(
            "bind={} scheme={} data_dir={} auth={} local_login={} oidc={} mcp={} gc={} \
             ratelimit=[{}] audit={} cluster={} seeds=[{}] log={}/{:?} otel={}",
            self.server.bind,
            if self.server.tls.is_enabled() { "https" } else { "http" },
            self.storage.data_dir.display(),
            if self.auth.insecure_no_auth { "DISABLED" } else { "enabled" },
            // The name as configured, not as parsed: the summary is printed
            // after validation, so the two agree, and printing the string
            // means a summary never claims a mode `validate` would refuse.
            self.auth.local.login,
            self.auth.oidc.describe(),
            if self.server.mcp { "enabled" } else { "off" },
            gc,
            self.server.rate_limit.describe(),
            self.audit.mode,
            if self.cluster.enabled { "enabled" } else { "single-node" },
            seeds,
            self.log.level,
            self.log.format,
            self.telemetry.describe(),
        )
    }
}

impl RateLimitConfig {
    fn validate(&self) -> Result<()> {
        // A window of zero would divide the burst by a clamped one-millisecond
        // window, producing a rate so high the limit is decorative. Rejecting
        // it is better than honouring a number that cannot mean what it says;
        // the way to turn a limiter off is to set its burst to zero.
        if self.login_per_ip > 0 && self.login_per_ip_window_secs == 0 {
            anyhow::bail!(
                "server.rate_limit.login_per_ip_window_secs must be greater than zero when \
                 login_per_ip is set; to disable the limit, set login_per_ip = 0"
            );
        }
        if self.login_per_user > 0 && self.login_per_user_window_secs == 0 {
            anyhow::bail!(
                "server.rate_limit.login_per_user_window_secs must be greater than zero when \
                 login_per_user is set; to disable the limit, set login_per_user = 0"
            );
        }
        if self.max_tracked_keys == 0 {
            anyhow::bail!(
                "server.rate_limit.max_tracked_keys must be greater than zero; a limiter that \
                 can remember nothing cannot limit anything"
            );
        }
        // An empty string is almost certainly meant as "no proxy", but it would
        // be read as a header whose name is empty and never match, so the
        // operator would believe forwarding was configured when it was not.
        if self.trusted_proxy_header.as_deref().is_some_and(str::is_empty) {
            anyhow::bail!(
                "server.rate_limit.trusted_proxy_header is empty; omit the setting to use the \
                 socket peer address, or name the header your proxy writes"
            );
        }
        Ok(())
    }

    /// Build the limiters the API layer holds.
    pub fn build(&self) -> kimmy_api::RateLimits {
        use std::time::Duration;
        kimmy_api::RateLimits {
            login_ip: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(
                    self.login_per_ip,
                    Duration::from_secs(self.login_per_ip_window_secs),
                ),
                self.max_tracked_keys,
            ),
            login_user: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(
                    self.login_per_user,
                    Duration::from_secs(self.login_per_user_window_secs),
                ),
                self.max_tracked_keys,
            ),
            // Lowercased because `http::HeaderMap` lookups are case-sensitive
            // over its canonical lowercase form, so `X-Forwarded-For` written
            // in a config file would otherwise silently never match.
            trusted_proxy_header: self.trusted_proxy_header.as_deref().map(str::to_lowercase),
        }
    }

    /// One-line form for the startup summary.
    fn describe(&self) -> String {
        let ip = if self.login_per_ip == 0 {
            "off".to_string()
        } else {
            format!("{}/{}s", self.login_per_ip, self.login_per_ip_window_secs)
        };
        let user = if self.login_per_user == 0 {
            "off".to_string()
        } else {
            format!("{}/{}s", self.login_per_user, self.login_per_user_window_secs)
        };
        format!("login_ip={ip} login_user={user}")
    }
}

fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Config {
        Config {
            auth: AuthConfig {
                root_password: Some("hunter2".into()),
                // Auth is on by default, and an auth-on node must be given a
                // signing key or it is refused — see `auth_requires_a_jwt_secret`.
                jwt_secret: Some("a-signing-key-of-adequate-length".into()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn defaults_round_trip_through_toml() {
        let text = toml::to_string(&Config::default()).unwrap();
        let parsed: Config = toml::from_str(&text).unwrap();
        assert_eq!(parsed, Config::default());
    }

    #[test]
    fn serializing_a_config_never_emits_a_secret() {
        // `check-config` prints this, and an operator reads it in a terminal, in
        // CI output, or pasted into a bug report. `jwt_secret` signs every local
        // token the cluster issues, so printing it hands over the ability to
        // mint any principal, `root` included.
        let cfg = valid();
        let secrets = [
            cfg.auth.root_password.clone().expect("the fixture sets a root password"),
            cfg.auth.jwt_secret.clone().expect("the fixture sets a signing key"),
        ];

        let text = toml::to_string(&cfg).unwrap();

        for secret in secrets {
            assert!(!text.contains(&secret), "a secret reached check-config output:\n{text}");
        }
        // Set-ness survives, because that is the question check-config answers.
        assert!(text.contains("root_password"), "the field vanished entirely:\n{text}");
        assert!(text.contains("jwt_secret"), "the field vanished entirely:\n{text}");
    }

    #[test]
    fn a_redacted_signing_key_is_refused_rather_than_used() {
        // check-config output pasted back into a config file must not start a
        // node whose tokens are signed with the placeholder.
        let mut cfg = valid();
        cfg.auth.jwt_secret = Some(REDACTED.into());

        let err = cfg.validate().unwrap_err().to_string();

        assert!(err.contains("jwt_secret"), "unhelpful error: {err}");
    }

    #[test]
    fn unknown_keys_are_rejected() {
        // A typo in a config file should fail loudly, not be silently ignored.
        let err = toml::from_str::<Config>("[server]\nbnid = \"0.0.0.0:1\"\n").unwrap_err();
        assert!(err.to_string().contains("bnid"), "unhelpful error: {err}");
    }

    #[test]
    fn a_complete_config_validates() {
        valid().validate().unwrap();
    }

    #[test]
    fn missing_root_password_is_rejected() {
        let err = Config::default().validate().unwrap_err().to_string();
        assert!(err.contains("KIMMY_ROOT_PASSWORD"), "unhelpful error: {err}");
    }

    #[test]
    fn no_auth_is_allowed_only_on_loopback() {
        let mut cfg = Config::default();
        cfg.auth.insecure_no_auth = true;

        cfg.server.bind = "0.0.0.0:7878".parse().unwrap();
        assert!(cfg.validate().is_err(), "must refuse to expose an unauthenticated server");

        cfg.server.bind = "127.0.0.1:7878".parse().unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn clustering_requires_seeds_and_secrets() {
        let mut cfg = valid();
        cfg.cluster.enabled = true;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("no seeds"), "unhelpful error: {err}");

        cfg.cluster.seeds = vec!["dns:seeds.internal".parse().unwrap()];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("cluster_secret"), "unhelpful error: {err}");

        cfg.cluster.cluster_secret = Some("shared".into());
        cfg.validate().unwrap();
    }

    #[test]
    fn auth_requires_a_jwt_secret() {
        // Without a configured secret an auth-on node falls back to a constant
        // compiled into the binary, so anyone could forge a root token. The
        // requirement holds for a single node, not just a cluster — a fresh
        // `docker run` on a routable address is exactly where it bites.
        let mut cfg = valid();
        cfg.cluster.enabled = false;
        cfg.auth.jwt_secret = None;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("jwt_secret"), "unhelpful error: {err}");
        assert!(err.contains("forge"), "the error should say what breaks: {err}");

        cfg.auth.jwt_secret = Some("a-signing-key-of-adequate-length".into());
        cfg.validate().unwrap();

        // The length rule is enforced where it is advertised, so `check-config`
        // refuses what the server would refuse rather than blessing a
        // configuration that dies after bootstrapping the superuser.
        cfg.auth.jwt_secret = Some("short".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("5 bytes"), "the error should name the length: {err}");
        assert!(
            kimmy_auth::TokenIssuer::new("short", 3600).is_err(),
            "validate must agree with the issuer about what is too short"
        );

        // With auth off there is no token to forge, so no secret is required —
        // this is what keeps loopback development a single flag.
        let mut off = Config::default();
        off.auth.insecure_no_auth = true;
        off.auth.jwt_secret = None;
        off.server.bind = "127.0.0.1:7878".parse().unwrap();
        off.validate().unwrap();
    }

    #[test]
    fn zero_retention_is_rejected_for_both_kinds() {
        let mut cfg = valid();
        cfg.storage.oplog_retention_secs = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid();
        cfg.storage.tombstone_retention_secs = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("resurrect"), "the error should say what breaks: {err}");
    }

    #[test]
    fn the_durability_class_is_one_of_two_and_the_window_is_bounded() {
        let mut cfg = valid();
        cfg.storage.durability = "fast".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("acknowledged write"), "{err}");
        cfg.storage.durability = "coalesced".into();
        cfg.validate().unwrap();
        cfg.storage.commit_coalesce_ms = 0;
        assert!(cfg.validate().is_err());
        cfg.storage.commit_coalesce_ms = 5000;
        assert!(cfg.validate().is_err());
        cfg.storage.commit_coalesce_ms = 1000;
        cfg.validate().unwrap();
    }

    #[test]
    fn the_multi_chunk_size_is_bounded() {
        let mut cfg = valid();
        cfg.storage.multi_chunk_docs = 0;
        assert!(cfg.validate().unwrap_err().to_string().contains("multi_chunk_docs"));
        cfg.storage.multi_chunk_docs = kimmy_storage::MAX_CANDIDATES + 1;
        assert!(cfg.validate().is_err());
        cfg.storage.multi_chunk_docs = kimmy_storage::MAX_CANDIDATES;
        cfg.validate().unwrap();
        cfg.storage.multi_chunk_docs = 1;
        cfg.validate().unwrap();
    }

    #[test]
    fn tombstone_retention_shorter_than_the_oplog_window_is_rejected() {
        // The one retention setting that makes a partition *shorter* than the
        // oplog window resurrect data: a peer replays the delete's entry
        // after the tombstone it needs to lose against is gone (ADR-085).
        let mut cfg = valid();
        cfg.storage.oplog_retention_secs = 3_600;
        cfg.storage.tombstone_retention_secs = 600;
        cfg.storage.gc_interval_secs = 60;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("resurrect"), "the error should say what breaks: {err}");

        // Equal is the floor, and longer is the recommendation.
        cfg.storage.tombstone_retention_secs = 3_600;
        cfg.validate().unwrap();
        cfg.storage.tombstone_retention_secs = 86_400;
        cfg.validate().unwrap();
    }

    #[test]
    fn a_collection_interval_longer_than_retention_is_rejected() {
        // Otherwise `oplog_retention_secs` silently means "retention plus up to
        // one interval", which is a number that does not mean what it says.
        let mut cfg = valid();
        cfg.storage.oplog_retention_secs = 60;
        cfg.storage.gc_interval_secs = 600;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("gc_interval_secs"), "unhelpful error: {err}");

        cfg.storage.gc_interval_secs = 60;
        cfg.validate().unwrap();
    }

    #[test]
    fn collection_can_be_disabled() {
        // Zero is a supported choice, not an oversight: an operator debugging
        // replication may want the history kept.
        let mut cfg = valid();
        cfg.storage.gc_interval_secs = 0;
        cfg.validate().unwrap();
        assert!(cfg.summary().contains("gc=off"));
    }

    #[test]
    fn webhook_delivery_limits_must_be_non_zero() {
        // Both are bounds on work, and zero of either does not mean "no bound"
        // — it means a dispatcher that can never send anything. Caught at
        // startup rather than as a webhook that silently never fires.
        let mut cfg = valid();
        cfg.webhooks.max_concurrent_deliveries = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("max_concurrent_deliveries"), "unhelpful error: {err}");

        let mut cfg = valid();
        cfg.webhooks.max_payload_bytes = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("max_payload_bytes"), "unhelpful error: {err}");
    }

    #[test]
    fn cluster_intervals_must_be_non_zero() {
        let mut cfg = valid();
        cfg.cluster.enabled = true;
        cfg.cluster.seeds = vec!["dns:seeds.internal".parse().unwrap()];
        cfg.cluster.cluster_secret = Some("shared".into());
        cfg.validate().unwrap();

        cfg.cluster.sync_interval_secs = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("sync_interval_secs"), "unhelpful error: {err}");

        cfg.cluster.sync_interval_secs = 5;
        cfg.cluster.discovery_interval_secs = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("discovery_interval_secs"), "unhelpful error: {err}");

        cfg.cluster.discovery_interval_secs = 30;
        cfg.cluster.fanout = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("fanout"), "unhelpful error: {err}");
    }

    #[test]
    fn tls_needs_both_halves_or_neither() {
        // Half-configured TLS would otherwise start and serve plaintext on a
        // port the operator believes is encrypted — the failure is silent from
        // the server's side, and only a client would notice.
        let mut cfg = valid();
        cfg.server.tls.cert_file = Some(PathBuf::from("/tmp/does-not-matter.crt"));
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("key_file"), "unhelpful error: {err}");

        let mut cfg = valid();
        cfg.server.tls.key_file = Some(PathBuf::from("/tmp/does-not-matter.key"));
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("cert_file"), "unhelpful error: {err}");
    }

    #[test]
    fn a_missing_certificate_is_refused_at_startup() {
        // Not at the first connection: the operator who can fix it is watching
        // the boot, not the traffic.
        let mut cfg = valid();
        cfg.server.tls.cert_file = Some(PathBuf::from("/nonexistent/server.crt"));
        cfg.server.tls.key_file = Some(PathBuf::from("/nonexistent/server.key"));

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("not a readable file"), "unhelpful error: {err}");
    }

    #[test]
    fn tls_is_enabled_by_naming_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("server.crt");
        let key = dir.path().join("server.key");
        std::fs::write(&cert, "x").unwrap();
        std::fs::write(&key, "x").unwrap();

        let mut cfg = valid();
        assert!(!cfg.server.tls.is_enabled(), "off by default");
        assert!(cfg.summary().contains("scheme=http "), "summary: {}", cfg.summary());

        cfg.server.tls.cert_file = Some(cert);
        cfg.server.tls.key_file = Some(key);
        cfg.validate().unwrap();

        assert!(cfg.server.tls.is_enabled());
        // The startup line is how an operator confirms which one is running.
        assert!(cfg.summary().contains("scheme=https"), "summary: {}", cfg.summary());
    }

    #[test]
    fn a_bad_audit_mode_is_refused_at_startup() {
        // Otherwise a typo produces a server that records nothing, which looks
        // exactly like a server nobody has attacked.
        let mut cfg = valid();
        cfg.audit.mode = "verbose".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("verbose"), "unhelpful error: {err}");
        assert!(err.contains("denials"), "the error should list valid modes: {err}");

        for mode in ["off", "denials", "writes", "all"] {
            cfg.audit.mode = mode.into();
            cfg.validate().unwrap_or_else(|e| panic!("{mode} should be valid: {e}"));
        }
    }

    fn oidc() -> OidcConfig {
        OidcConfig {
            issuer: Some("https://auth.example.com".into()),
            audience: Some("kimmydb".into()),
            role_mappings: vec![kimmy_auth::RoleMapping {
                role: None,
                claim_value: "kimmydb-analyst".into(),
                grants: vec![kimmy_auth::Grant::new(
                    "sales",
                    "orders*",
                    vec![kimmy_auth::Action::Read, kimmy_auth::Action::Search],
                )],
            }],
            ..Default::default()
        }
    }

    #[test]
    fn local_login_defaults_to_always_and_every_name_parses() {
        // The default is what shipped, and it is the one mode with nothing to
        // say at startup. The summary still names it, so an operator reading
        // the boot line sees the mode rather than inferring it.
        let cfg = valid();
        assert_eq!(cfg.auth.local.login, "always");
        assert_eq!(cfg.auth.local.login_mode().unwrap(), kimmy_api::LocalLogin::Always);
        assert!(cfg.summary().contains("local_login=always"), "{}", cfg.summary());

        let mut cfg = valid();
        cfg.auth.local.login = "loopback_only".into();
        cfg.validate().unwrap();
        assert!(cfg.summary().contains("local_login=loopback_only"), "{}", cfg.summary());
    }

    #[test]
    fn an_unknown_local_login_mode_is_refused_with_the_valid_ones_named() {
        // A typo must not quietly mean "always", which is the permissive end.
        let mut cfg = valid();
        cfg.auth.local.login = "localhost".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("auth.local"), "name the setting: {err}");
        assert!(err.contains("localhost"), "name the value: {err}");
        assert!(err.contains("loopback_only") && err.contains("disabled"), "list the modes: {err}");
    }

    #[test]
    fn disabling_local_login_needs_an_identity_provider() {
        // With the password route gone and no provider, nobody could ever log
        // in — refused at startup rather than discovered at the first 404.
        let mut cfg = valid();
        cfg.auth.local.login = "disabled".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("auth.local.login"), "unhelpful error: {err}");
        assert!(err.contains("auth.oidc"), "say what would fix it: {err}");
        assert!(err.contains("KIMMY_LOCAL_LOGIN"), "name the variable: {err}");

        // With a provider it is a legitimate choice.
        cfg.auth.oidc = oidc();
        cfg.validate().unwrap();
        assert!(cfg.summary().contains("local_login=disabled"), "{}", cfg.summary());

        // `loopback_only` never needs one: the login is still reachable, from
        // the host.
        let mut cfg = valid();
        cfg.auth.local.login = "loopback_only".into();
        cfg.validate().unwrap();
    }

    #[test]
    fn local_login_reads_back_from_toml_as_documented() {
        let cfg: Config = toml::from_str(
            "[auth]\nroot_password = \"hunter2\"\n\
             jwt_secret = \"a-signing-key-of-adequate-length\"\n\
             [auth.local]\nlogin = \"loopback_only\"\n",
        )
        .unwrap();
        assert_eq!(cfg.auth.local.login_mode().unwrap(), kimmy_api::LocalLogin::LoopbackOnly);
        cfg.validate().unwrap();

        // And an unknown key under the section is a typo, like everywhere else.
        let err = toml::from_str::<Config>("[auth.local]\nlogin_mode = \"always\"\n").unwrap_err();
        assert!(err.to_string().contains("login_mode"), "unhelpful error: {err}");
    }

    #[test]
    fn the_subject_claim_is_optional_reaches_the_verifier_and_shows_in_the_summary() {
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        assert_eq!(cfg.auth.oidc.subject_claim, None, "unset by default: the subject is shown");
        assert_eq!(cfg.auth.oidc.settings().unwrap().subject_claim, None);
        assert!(!cfg.summary().contains("display from"), "{}", cfg.summary());

        cfg.auth.oidc.subject_claim = Some("email".into());
        cfg.validate().unwrap();
        assert_eq!(cfg.auth.oidc.settings().unwrap().subject_claim.as_deref(), Some("email"));
        assert!(cfg.summary().contains("display from email"), "{}", cfg.summary());

        // From the file, in the documented spelling.
        let cfg: Config = toml::from_str(
            "[auth.oidc]\nissuer = \"https://auth.example.com\"\naudience = \"kimmydb\"\n\
             subject_claim = \"preferred_username\"\n",
        )
        .unwrap();
        assert_eq!(cfg.auth.oidc.subject_claim.as_deref(), Some("preferred_username"));
    }

    #[test]
    fn an_empty_subject_claim_is_refused_rather_than_read_as_unset() {
        // An operator who wrote the key meant to name a claim; a blank one
        // would silently mean "the subject", which is what they were trying to
        // get away from.
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        cfg.auth.oidc.subject_claim = Some("  ".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("auth.oidc.subject_claim"), "unhelpful error: {err}");
        assert!(err.contains("KIMMY_OIDC_SUBJECT_CLAIM"), "name the variable: {err}");
    }

    #[test]
    fn a_complete_oidc_section_validates_and_shows_in_the_summary() {
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        cfg.validate().unwrap();

        // The startup line is how an operator confirms the file was read at
        // all — a mistyped section name would otherwise be a silent no-op.
        let summary = cfg.summary();
        assert!(summary.contains("oidc=https://auth.example.com"), "{summary}");
        assert!(summary.contains("1 role mappings"), "{summary}");
        assert!(Config::default().summary().contains("oidc=off"));
    }

    #[test]
    fn oidc_needs_both_an_issuer_and_an_audience() {
        // Half a section is unambiguously a mistake, and the useful moment to
        // say so is at startup: the alternative is a node that starts, ignores
        // the section, and refuses every token the provider issues.
        let mut cfg = valid();
        cfg.auth.oidc.audience = Some("kimmydb".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("no issuer"), "unhelpful error: {err}");
        assert!(err.contains("refused"), "the error should say what breaks: {err}");

        let mut cfg = valid();
        cfg.auth.oidc.issuer = Some("https://auth.example.com".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("audience"), "unhelpful error: {err}");
        assert!(err.contains("unrelated application"), "the error should say what breaks: {err}");
    }

    #[test]
    fn a_plaintext_issuer_is_refused() {
        // The discovery document and the signing keys are fetched from this
        // URL, so over plaintext anyone on the path can substitute their own
        // keys — which is a way to mint any identity they like.
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        cfg.auth.oidc.issuer = Some("http://auth.example.com".into());

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("not https"), "unhelpful error: {err}");
        assert!(err.contains("substitute their own keys"), "{err}");
    }

    #[test]
    fn a_role_mapping_that_grants_admin_is_refused_at_startup() {
        // `admin` is reserved to local users as a break-glass boundary: if the
        // identity provider is misconfigured or taken over, nobody gets
        // superuser over KimmyDB through it (ADR-067).
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        cfg.auth.oidc.role_mappings.push(kimmy_auth::RoleMapping {
            role: None,
            claim_value: "kimmydb-admin".into(),
            grants: vec![kimmy_auth::Grant::superuser()],
        });

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("kimmydb-admin"), "the error should name the mapping: {err}");
        assert!(err.contains("reserved"), "the error should say what breaks: {err}");

        // Every other action stays mappable — the boundary is `admin` alone.
        cfg.auth.oidc.role_mappings.pop();
        cfg.auth.oidc.role_mappings.push(kimmy_auth::RoleMapping {
            role: None,
            claim_value: "kimmydb-writer".into(),
            grants: vec![kimmy_auth::Grant::new(
                "sales",
                "*",
                vec![
                    kimmy_auth::Action::Write,
                    kimmy_auth::Action::Watch,
                    kimmy_auth::Action::Webhook,
                ],
            )],
        });
        cfg.validate().unwrap();
    }

    #[test]
    fn a_role_mapping_naming_an_unknown_action_is_refused_while_the_file_is_read() {
        // Earlier than `validate`, because an action is a typed enum: the value
        // cannot even be represented, so the refusal is a parse error. Asserted
        // anyway, because "is it caught at all" is the question, and the answer
        // has to include the valid values or the operator is left guessing.
        let err = toml::from_str::<Config>(
            "[auth.oidc]\nissuer = \"https://auth.example.com\"\n\
             [[auth.oidc.role_mappings]]\nclaim_value = \"analyst\"\n\
             grants = [{ db = \"sales\", collection = \"*\", actions = [\"delet\"] }]\n",
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("delet"), "the error should name the bad value: {err}");
        assert!(err.contains("read"), "the error should list the valid ones: {err}");
    }

    #[test]
    fn an_oidc_section_reads_back_from_toml_as_written() {
        // The documented shape, exactly as `kimmy.example.toml` shows it. A
        // renamed field would otherwise be caught only by an operator whose
        // node refuses to start.
        let cfg: Config = toml::from_str(
            "[auth.oidc]\n\
             issuer = \"https://auth.example.com\"\n\
             audience = \"kimmydb\"\n\
             roles_claim = \"roles\"\n\
             [[auth.oidc.role_mappings]]\n\
             claim_value = \"kimmydb-analyst\"\n\
             grants = [{ db = \"sales\", collection = \"orders*\", actions = [\"read\", \"search\"] }]\n",
        )
        .unwrap();

        let settings = cfg.auth.oidc.settings().expect("issuer and audience are both set");
        assert_eq!(settings.issuer, "https://auth.example.com");
        assert_eq!(settings.audience, "kimmydb");
        assert_eq!(settings.roles_claim, "roles");
        assert_eq!(settings.role_mappings.len(), 1);
        assert_eq!(settings.role_mappings[0].claim_value, "kimmydb-analyst");
        assert_eq!(settings.role_mappings[0].grants[0].collection, "orders*");
    }

    #[test]
    fn federation_is_refused_with_authentication_disabled() {
        // Every request is already a superuser, so the role mappings would be
        // enforcing nothing while looking as though they were.
        let mut cfg = Config::default();
        cfg.auth.insecure_no_auth = true;
        cfg.server.bind = "127.0.0.1:7878".parse().unwrap();
        cfg.validate().unwrap();

        cfg.auth.oidc = oidc();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("insecure_no_auth"), "unhelpful error: {err}");
    }

    #[test]
    fn a_zero_key_refresh_interval_is_refused() {
        // A node that never re-fetches would refuse every token the moment the
        // provider rotated its keys.
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        cfg.auth.oidc.refresh_interval_secs = 0;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("refresh_interval_secs"), "unhelpful error: {err}");
    }

    #[test]
    fn an_empty_roles_claim_is_refused() {
        // It would read no claim, so every federated caller would arrive with
        // no grants — which looks exactly like a permissions problem.
        let mut cfg = valid();
        cfg.auth.oidc = oidc();
        cfg.auth.oidc.roles_claim = "  ".into();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("roles_claim"), "unhelpful error: {err}");
        assert!(err.contains("groups"), "the error should name the Entra ID case: {err}");
    }

    fn telemetry() -> TelemetryConfig {
        TelemetryConfig {
            endpoint: Some("http://otel-collector:4318".into()),
            ..Default::default()
        }
    }

    #[test]
    fn telemetry_is_off_by_default_and_a_configured_endpoint_shows_in_the_summary() {
        // Off is the default, and the startup line is how an operator confirms
        // the section was read at all — a mistyped table name would otherwise
        // be a silent no-op that looks exactly like a collector that is down.
        let cfg = valid();
        assert!(!cfg.telemetry.is_configured());
        assert!(cfg.summary().contains("otel=off"), "{}", cfg.summary());

        let mut cfg = valid();
        cfg.telemetry = telemetry();
        cfg.validate().unwrap();

        let summary = cfg.summary();
        assert!(summary.contains("otel=http://otel-collector:4318"), "{summary}");
        assert!(summary.contains("http/protobuf"), "{summary}");
        // The one setting that changes what leaves the process, named where an
        // operator can see which of the two they turned on (ADR-068).
        assert!(summary.contains("names=off"), "{summary}");

        cfg.telemetry.include_names = true;
        assert!(cfg.summary().contains("names=on"), "{}", cfg.summary());
    }

    #[test]
    fn a_relative_telemetry_endpoint_is_refused() {
        // It is a base a signal path is appended to, so a bare host and port
        // has nowhere to send anything.
        let mut cfg = valid();
        cfg.telemetry.endpoint = Some("otel-collector:4318".into());

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("telemetry.endpoint"), "unhelpful error: {err}");
        assert!(err.contains("KIMMY_OTLP_ENDPOINT"), "the error should name the env var: {err}");
        assert!(err.contains("absolute"), "the error should say what breaks: {err}");
    }

    #[test]
    fn an_https_telemetry_endpoint_is_refused_rather_than_failing_at_the_first_export() {
        // The exporter is built without a TLS backend (ADR-069), so an https
        // endpoint is a node that serves perfectly while every export fails —
        // indistinguishable from a collector nobody configured.
        let mut cfg = valid();
        cfg.telemetry.endpoint = Some("https://otel-collector:4318".into());

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("telemetry.endpoint"), "unhelpful error: {err}");
        assert!(err.contains("TLS backend"), "the error should say what breaks: {err}");
        assert!(err.contains("ADR-069"), "the error should point at the decision: {err}");
    }

    #[test]
    fn an_unknown_telemetry_protocol_lists_the_two_and_says_grpc_is_deliberate() {
        // Otherwise "grpc" reads as a typo in this file rather than as a
        // transport this build does not have.
        let mut cfg = valid();
        cfg.telemetry = telemetry();
        cfg.telemetry.protocol = "grpc".into();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("telemetry.protocol"), "unhelpful error: {err}");
        assert!(err.contains("http/protobuf"), "the error should list the valid ones: {err}");
        assert!(err.contains("http/json"), "the error should list the valid ones: {err}");
        assert!(err.contains("ADR-069"), "the error should point at the decision: {err}");

        for protocol in ["http/protobuf", "http/json"] {
            cfg.telemetry.protocol = protocol.into();
            cfg.validate().unwrap_or_else(|e| panic!("{protocol} should be valid: {e}"));
        }
    }

    #[test]
    fn a_sample_ratio_outside_zero_to_one_is_refused() {
        let mut cfg = valid();
        cfg.telemetry = telemetry();

        for bad in [-0.5, 1.5, f64::NAN, f64::INFINITY] {
            cfg.telemetry.sample_ratio = bad;
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains("telemetry.sample_ratio"), "unhelpful error for {bad}: {err}");
            assert!(err.contains("KIMMY_OTLP_SAMPLE_RATIO"), "name the env var for {bad}: {err}");
        }

        cfg.telemetry.sample_ratio = 0.25;
        cfg.validate().unwrap();
    }

    #[test]
    fn an_endpoint_with_a_zero_sample_ratio_is_refused_rather_than_being_a_silent_no_op() {
        // It configures an exporter, a batch processor and a connection to a
        // collector that can never emit a single span. The operator would find
        // that out from an empty dashboard weeks later; the useful moment to
        // say so is while one is watching the boot.
        let mut cfg = valid();
        cfg.telemetry = telemetry();
        cfg.telemetry.sample_ratio = 0.0;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("sample_ratio"), "unhelpful error: {err}");
        assert!(err.contains("never emit"), "the error should say what breaks: {err}");
        assert!(err.contains("remove telemetry.endpoint"), "the error should say the fix: {err}");

        // And with no endpoint it is inert, not an error: nothing is exported
        // either way, so a leftover zero must not stop a node from starting.
        let mut off = valid();
        off.telemetry.sample_ratio = 0.0;
        off.validate().unwrap();
    }

    #[test]
    fn a_zero_export_timeout_is_refused() {
        let mut cfg = valid();
        cfg.telemetry = telemetry();
        cfg.telemetry.export_timeout_secs = 0;

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("export_timeout_secs"), "unhelpful error: {err}");
        assert!(err.contains("time out"), "the error should say what breaks: {err}");
    }

    #[test]
    fn an_empty_service_name_is_refused() {
        // Every span would arrive under a blank service, indistinguishable
        // from anything else reporting to the same collector.
        let mut cfg = valid();
        cfg.telemetry = telemetry();
        cfg.telemetry.service_name = "  ".into();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("telemetry.service_name"), "unhelpful error: {err}");
        assert!(
            err.contains("KIMMY_OTLP_SERVICE_NAME"),
            "the error should name the env var: {err}"
        );
    }

    #[test]
    fn a_telemetry_section_reads_back_from_toml_as_written() {
        // The documented shape, exactly as `kimmy.example.toml` shows it. A
        // renamed field would otherwise be caught only by an operator whose
        // node refuses to start.
        let cfg: Config = toml::from_str(
            "[telemetry]\n\
             endpoint = \"http://otel-collector:4318\"\n\
             protocol = \"http/json\"\n\
             sample_ratio = 0.1\n\
             include_names = true\n\
             service_name = \"kimmydb-prod\"\n\
             export_timeout_secs = 5\n",
        )
        .unwrap();

        assert_eq!(cfg.telemetry.endpoint.as_deref(), Some("http://otel-collector:4318"));
        assert_eq!(cfg.telemetry.protocol, "http/json");
        assert_eq!(cfg.telemetry.sample_ratio, 0.1);
        assert!(cfg.telemetry.include_names);
        assert_eq!(cfg.telemetry.service_name, "kimmydb-prod");
        assert_eq!(cfg.telemetry.export_timeout_secs, 5);
    }

    #[test]
    fn summary_does_not_leak_secrets() {
        let mut cfg = valid();
        cfg.auth.jwt_secret = Some("super-secret-signing-key".into());
        cfg.cluster.cluster_secret = Some("super-secret-cluster-key".into());
        let summary = cfg.summary();
        assert!(!summary.contains("hunter2"));
        assert!(!summary.contains("super-secret"));
    }
}
