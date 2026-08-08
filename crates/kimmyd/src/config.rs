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
            "bind={} data_dir={} auth={} mcp={} gc={} cluster={} seeds=[{}] log={}/{:?}",
            self.server.bind,
            self.storage.data_dir.display(),
            if self.auth.insecure_no_auth { "DISABLED" } else { "enabled" },
            if self.server.mcp { "enabled" } else { "off" },
            gc,
            if self.cluster.enabled { "enabled" } else { "single-node" },
            seeds,
            self.log.level,
            self.log.format,
        )
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
    fn summary_does_not_leak_secrets() {
        let mut cfg = valid();
        cfg.auth.jwt_secret = Some("super-secret-signing-key".into());
        cfg.cluster.cluster_secret = Some("super-secret-cluster-key".into());
        let summary = cfg.summary();
        assert!(!summary.contains("hunter2"));
        assert!(!summary.contains("super-secret"));
    }
}
