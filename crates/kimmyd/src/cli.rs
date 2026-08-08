//! Command-line interface and config layering.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use kimmy_cluster::SeedSource;

use crate::config::{Config, LogFormat};

#[derive(Parser, Debug)]
#[command(
    name = "kimmyd",
    version,
    about = "KimmyDB — a leaderless JSON document database with change streams, \
             vector search, and a built-in MCP server"
)]
pub struct Cli {
    /// Path to a TOML config file. Flags and environment variables override it.
    #[arg(short, long, env = "KIMMY_CONFIG", global = true)]
    pub config: Option<PathBuf>,

    #[command(flatten)]
    pub overrides: Overrides,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the server (the default when no subcommand is given).
    Run,
    /// Validate the resolved configuration and print it, then exit.
    ///
    /// Useful in CI and in a container entrypoint to fail fast on a bad mount.
    CheckConfig,
}

/// Flags that override config-file values. Every one is optional so that
/// "not passed" is distinguishable from "passed the default value".
#[derive(clap::Args, Debug, Default)]
pub struct Overrides {
    /// Address to serve HTTP, WebSocket, and MCP on.
    #[arg(short, long, env = "KIMMY_BIND")]
    pub bind: Option<SocketAddr>,

    /// Directory for the database file and node identity.
    #[arg(short, long, env = "KIMMY_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Do not serve the MCP endpoint at /mcp.
    #[arg(long, env = "KIMMY_NO_MCP")]
    pub no_mcp: bool,

    /// Bootstrap superuser name, created on first start.
    #[arg(long, env = "KIMMY_ROOT_USER")]
    pub root_user: Option<String>,

    /// Bootstrap superuser password. Prefer the environment variable.
    #[arg(long, env = "KIMMY_ROOT_PASSWORD", hide_env_values = true)]
    pub root_password: Option<String>,

    /// JWT signing secret. Must be identical on every node in a cluster.
    #[arg(long, env = "KIMMY_JWT_SECRET", hide_env_values = true)]
    pub jwt_secret: Option<String>,

    /// Run with authentication disabled. Only permitted on a loopback bind.
    #[arg(long, env = "KIMMY_INSECURE_NO_AUTH")]
    pub insecure_no_auth: bool,

    /// Join a cluster.
    #[arg(long, env = "KIMMY_CLUSTER_ENABLED")]
    pub cluster: bool,

    /// Address for the cluster replication transport.
    #[arg(long, env = "KIMMY_CLUSTER_BIND")]
    pub cluster_bind: Option<SocketAddr>,

    /// Shared secret authenticating node-to-node traffic.
    #[arg(long, env = "KIMMY_CLUSTER_SECRET", hide_env_values = true)]
    pub cluster_secret: Option<String>,

    /// Where to look for peers. Repeatable or comma-separated. Accepts
    /// `k8s:<headless-service>`, `dns:<name>`, `dns-srv:<name>`,
    /// `static:<host:port,...>`, or a bare `host:port`.
    #[arg(long, env = "KIMMY_SEEDS", value_delimiter = ',')]
    pub seeds: Vec<SeedSource>,

    /// Log filter directive, e.g. `info` or `info,kimmy_storage=debug`.
    #[arg(long, env = "KIMMY_LOG_LEVEL")]
    pub log_level: Option<String>,

    /// Log output format.
    #[arg(long, env = "KIMMY_LOG_FORMAT", value_enum)]
    pub log_format: Option<LogFormatArg>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum LogFormatArg {
    Pretty,
    Json,
}

impl From<LogFormatArg> for LogFormat {
    fn from(a: LogFormatArg) -> Self {
        match a {
            LogFormatArg::Pretty => LogFormat::Pretty,
            LogFormatArg::Json => LogFormat::Json,
        }
    }
}

impl Cli {
    /// Resolve the effective configuration: defaults, then the file, then these
    /// flags.
    pub fn resolve(&self) -> Result<Config> {
        let mut cfg = match &self.config {
            Some(path) => Config::load(path)?,
            None => Config::default(),
        };
        self.overrides.apply(&mut cfg);
        cfg.validate()?;
        Ok(cfg)
    }
}

impl Overrides {
    fn apply(&self, cfg: &mut Config) {
        if let Some(bind) = self.bind {
            cfg.server.bind = bind;
        }
        if let Some(dir) = &self.data_dir {
            cfg.storage.data_dir = dir.clone();
        }
        if let Some(user) = &self.root_user {
            cfg.auth.root_user = user.clone();
        }
        if let Some(pw) = &self.root_password {
            cfg.auth.root_password = Some(pw.clone());
        }
        if let Some(secret) = &self.jwt_secret {
            cfg.auth.jwt_secret = Some(secret.clone());
        }
        // Boolean flags are one-way: passing `--insecure-no-auth` turns the
        // setting on, but omitting it must not silently turn off what the
        // config file asked for.
        if self.insecure_no_auth {
            cfg.auth.insecure_no_auth = true;
        }
        // Phrased as `--no-mcp` rather than `--mcp` for the same reason: the
        // flag can only turn the endpoint off, so omitting it cannot override a
        // config file that already disabled it.
        if self.no_mcp {
            cfg.server.mcp = false;
        }
        if self.cluster {
            cfg.cluster.enabled = true;
        }
        if let Some(bind) = self.cluster_bind {
            cfg.cluster.bind = bind;
        }
        if let Some(secret) = &self.cluster_secret {
            cfg.cluster.cluster_secret = Some(secret.clone());
        }
        if !self.seeds.is_empty() {
            cfg.cluster.seeds = self.seeds.clone();
            // Naming seeds is unambiguous intent to cluster; requiring a
            // separate --cluster flag alongside would only be a papercut.
            cfg.cluster.enabled = true;
        }
        if let Some(level) = &self.log_level {
            cfg.log.level = level.clone();
        }
        if let Some(format) = self.log_format {
            cfg.log.format = format.into();
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("kimmyd").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("failed to parse {args:?}: {e}"))
    }

    #[test]
    fn flags_override_defaults() {
        let cli = parse(&["--bind", "127.0.0.1:9999", "--data-dir", "/tmp/kimmy"]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg);
        assert_eq!(cfg.server.bind, "127.0.0.1:9999".parse().unwrap());
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/tmp/kimmy"));
    }

    #[test]
    fn omitted_boolean_does_not_clear_the_config_file() {
        let cli = parse(&[]);
        let mut cfg = Config::default();
        cfg.auth.insecure_no_auth = true;
        cli.overrides.apply(&mut cfg);
        assert!(cfg.auth.insecure_no_auth, "an absent flag must not override the file");
    }

    #[test]
    fn seeds_parse_and_imply_clustering() {
        let cli = parse(&["--seeds", "k8s:kimmy-headless.default.svc.cluster.local"]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg);
        assert!(cfg.cluster.enabled, "naming seeds should enable clustering");
        assert_eq!(cfg.cluster.seeds.len(), 1);
    }

    #[test]
    fn seeds_accept_a_comma_separated_list() {
        let cli = parse(&["--seeds", "10.0.0.1:7900,dns:seeds.internal"]);
        assert_eq!(cli.overrides.seeds.len(), 2);
    }

    #[test]
    fn a_bad_seed_is_a_parse_error() {
        assert!(Cli::try_parse_from(["kimmyd", "--seeds", "static:garbage"]).is_err());
    }
}
