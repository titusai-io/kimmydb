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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub cluster: ClusterConfig,
    pub webhooks: WebhookConfig,
    pub audit: AuditConfig,
    pub log: LogConfig,
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
    /// How often to collect records that are past their retention.
    ///
    /// Separate from the retention windows themselves: retention says what is
    /// garbage, this says how often to look. Zero disables collection, which
    /// restores the pre-M5 behaviour of unbounded growth — available because an
    /// operator debugging a replication problem may want the history kept.
    pub gc_interval_secs: u64,
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
    pub root_password: Option<String>,
    /// Shared secret for signing JWTs. Every node in a cluster must agree, or
    /// tokens issued by one node will be rejected by another.
    pub jwt_secret: Option<String>,
    pub token_ttl_secs: u64,
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

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:7878".parse().expect("valid literal"),
            mcp: true,
            mcp_allowed_hosts: Vec::new(),
            rate_limit: RateLimitConfig::default(),
            tls: TlsConfig::default(),
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
            // Frequent enough that disk use tracks retention rather than
            // sawtoothing, rare enough that the scan is not a background load.
            gc_interval_secs: 10 * 60,
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
            // Tokens must validate on every node, so the signing key cannot be
            // per-node. Catching this here avoids a confusing intermittent 401
            // that only shows up when a request lands on the "wrong" node.
            if !self.auth.insecure_no_auth && self.auth.jwt_secret.is_none() {
                anyhow::bail!(
                    "cluster.enabled is set but no auth.jwt_secret is configured. Every node \
                     must sign tokens with the same key or tokens issued by one node will be \
                     rejected by the others. Set KIMMY_JWT_SECRET identically on all nodes."
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
        // Parsed at startup so a typo is a boot failure rather than an audit
        // log that silently records nothing.
        kimmy_api::AuditMode::parse(&self.audit.mode).map_err(|e| anyhow::anyhow!("audit.{e}"))?;

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
            "bind={} scheme={} data_dir={} auth={} mcp={} gc={} ratelimit=[{}] audit={} \
             cluster={} seeds=[{}] log={}/{:?}",
            self.server.bind,
            if self.server.tls.is_enabled() { "https" } else { "http" },
            self.storage.data_dir.display(),
            if self.auth.insecure_no_auth { "DISABLED" } else { "enabled" },
            if self.server.mcp { "enabled" } else { "off" },
            gc,
            self.server.rate_limit.describe(),
            self.audit.mode,
            if self.cluster.enabled { "enabled" } else { "single-node" },
            seeds,
            self.log.level,
            self.log.format,
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
            auth: AuthConfig { root_password: Some("hunter2".into()), ..Default::default() },
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
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("jwt_secret"), "unhelpful error: {err}");

        cfg.auth.jwt_secret = Some("signing-key".into());
        cfg.validate().unwrap();
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
        cfg.auth.jwt_secret = Some("signing-key".into());
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
