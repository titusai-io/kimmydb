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
    /// Address the gossip transport binds to.
    pub bind: SocketAddr,
    /// Where to look for peers. Re-resolved periodically, so a Kubernetes
    /// headless service picks up new pods without a restart.
    pub seeds: Vec<SeedSource>,
    /// Shared secret authenticating node-to-node traffic.
    pub cluster_secret: Option<String>,
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
            bind: format!("0.0.0.0:{}", kimmy_cluster::DEFAULT_GOSSIP_PORT)
                .parse()
                .expect("valid literal"),
            seeds: Vec::new(),
            cluster_secret: None,
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

        if self.storage.oplog_retention_secs == 0 {
            anyhow::bail!("storage.oplog_retention_secs must be greater than zero");
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
        format!(
            "bind={} data_dir={} auth={} mcp={} cluster={} seeds=[{}] log={}/{:?}",
            self.server.bind,
            self.storage.data_dir.display(),
            if self.auth.insecure_no_auth { "DISABLED" } else { "enabled" },
            if self.server.mcp { "enabled" } else { "off" },
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
    fn summary_does_not_leak_secrets() {
        let mut cfg = valid();
        cfg.auth.jwt_secret = Some("super-secret-signing-key".into());
        cfg.cluster.cluster_secret = Some("super-secret-cluster-key".into());
        let summary = cfg.summary();
        assert!(!summary.contains("hunter2"));
        assert!(!summary.contains("super-secret"));
    }
}
