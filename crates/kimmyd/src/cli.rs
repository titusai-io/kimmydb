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
    version = kimmy_core::build::ident(),
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
    /// Restore a backup into a new data directory, then exit.
    ///
    /// Offline by necessity: redb allows one process to hold a database, so a
    /// restore cannot run against a node that is serving. Take the backup with
    /// `GET /v1/admin/backup` while the node runs; put it back with this while
    /// it does not.
    Restore {
        /// The backup file, as written by `GET /v1/admin/backup`.
        #[arg(long)]
        from: PathBuf,
        /// Restore to an earlier instant, as milliseconds since the epoch.
        ///
        /// Point-in-time restore. The backup is written out, then documents
        /// changed after this instant are put back to the value the oplog says
        /// they held at it. Refuses, without writing, if the target predates
        /// the oplog horizon, if a schema change happened after it, or if any
        /// document's earlier value has already been collected.
        #[arg(long)]
        until: Option<u64>,
    },
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

    /// Do not run the automatic embedding worker on this node.
    ///
    /// Vectors still replicate in from nodes that do run workers, and vector
    /// search keeps working. One-way, like `--no-mcp`: the flag can only turn
    /// the worker off, so omitting it cannot override a config file that
    /// already disabled it.
    #[arg(long, env = "KIMMY_DISABLE_VECTOR_WORKER")]
    pub disable_vector_worker: bool,

    /// Issuer URL of an external OIDC provider to federate with. Must be https.
    #[arg(long, env = "KIMMY_OIDC_ISSUER")]
    pub oidc_issuer: Option<String>,

    /// The `aud` a federated token must carry. Required with --oidc-issuer.
    #[arg(long, env = "KIMMY_OIDC_AUDIENCE")]
    pub oidc_audience: Option<String>,

    /// Claim carrying the caller's roles. `groups` for Entra ID.
    ///
    /// There is deliberately no way to spell individual mappings as repeated
    /// flags: a grant is a structure, and a command line is where structures
    /// go to be mistyped. They live in the config file (ADR-066), or in one
    /// JSON document through --oidc-role-mappings (ADR-078).
    #[arg(long, env = "KIMMY_OIDC_ROLES_CLAIM")]
    pub oidc_roles_claim: Option<String>,

    /// Claim values and what each earns here, overriding the config file.
    ///
    /// One JSON array of role mappings, aimed at deployments that configure
    /// the node through an environment block — compose, swarm, kubernetes —
    /// where editing a TOML file inside a container is not realistic:
    ///
    /// ```text
    /// KIMMY_OIDC_ROLE_MAPPINGS='[{"claim_value":"developer","role":"analyst"}]'
    /// ```
    ///
    /// Each mapping carries `claim_value` plus `role` and/or `grants`, exactly
    /// as in the file, and every startup refusal applies unchanged. When set,
    /// the variable **replaces** the file's list rather than merging with it,
    /// so what runs is exactly what was passed.
    #[arg(long, env = "KIMMY_OIDC_ROLE_MAPPINGS")]
    pub oidc_role_mappings: Option<String>,

    /// How often to re-fetch the provider's signing keys, in seconds.
    #[arg(long, env = "KIMMY_OIDC_REFRESH_INTERVAL_SECS")]
    pub oidc_refresh_interval_secs: Option<u64>,

    /// The longest a federated token may be valid for by its own exp - iat,
    /// in seconds. Default 900; refused outside 1..=86400 (ADR-096).
    #[arg(long, env = "KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS")]
    pub oidc_max_token_lifetime_secs: Option<u64>,

    /// PEM certificate chain, leaf first. Enables TLS together with --tls-key.
    #[arg(long, env = "KIMMY_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// PEM private key (PKCS#8, PKCS#1 or SEC1).
    #[arg(long, env = "KIMMY_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

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

    /// Base URL of an OpenTelemetry collector, e.g. http://otel-collector:4318.
    ///
    /// Setting it is what turns telemetry on; there is no separate enable flag.
    #[arg(long, env = "KIMMY_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,

    /// OTLP encoding: `http/protobuf` (default) or `http/json`. No gRPC.
    #[arg(long, env = "KIMMY_OTLP_PROTOCOL")]
    pub otlp_protocol: Option<String>,

    /// Fraction of traces to record, 0.0 to 1.0.
    #[arg(long, env = "KIMMY_OTLP_SAMPLE_RATIO")]
    pub otlp_sample_ratio: Option<f64>,

    /// What `service.name` this node reports itself as.
    #[arg(long, env = "KIMMY_OTLP_SERVICE_NAME")]
    pub otlp_service_name: Option<String>,

    /// Let spans carry database and collection names.
    ///
    /// Off by default. Turning it on publishes a deployment's schema to
    /// whatever holds the traces — see ADR-068 and docs/security.md.
    #[arg(long, env = "KIMMY_TELEMETRY_INCLUDE_NAMES")]
    pub telemetry_include_names: bool,
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
        self.overrides.apply(&mut cfg)?;

        // `restore` moves a file into a data directory and exits. It never
        // serves, never authenticates anybody, and never joins a cluster, so
        // holding it to the serving configuration would mean an operator
        // recovering from an incident has to supply a root password to a
        // command that will not use one. Found by running it.
        if !matches!(self.command, Some(Command::Restore { .. })) {
            cfg.validate()?;
        }
        Ok(cfg)
    }
}

impl Overrides {
    fn apply(&self, cfg: &mut Config) -> Result<()> {
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
        if let Some(issuer) = &self.oidc_issuer {
            cfg.auth.oidc.issuer = Some(issuer.clone());
        }
        if let Some(audience) = &self.oidc_audience {
            cfg.auth.oidc.audience = Some(audience.clone());
        }
        if let Some(claim) = &self.oidc_roles_claim {
            cfg.auth.oidc.roles_claim = claim.clone();
        }
        if let Some(raw) = &self.oidc_role_mappings {
            cfg.auth.oidc.role_mappings = parse_role_mappings(raw)?;
        }
        if let Some(secs) = self.oidc_refresh_interval_secs {
            cfg.auth.oidc.refresh_interval_secs = secs;
        }
        if let Some(secs) = self.oidc_max_token_lifetime_secs {
            cfg.auth.oidc.max_token_lifetime_secs = secs;
        }
        if let Some(cert) = &self.tls_cert {
            cfg.server.tls.cert_file = Some(cert.clone());
        }
        if let Some(key) = &self.tls_key {
            cfg.server.tls.key_file = Some(key.clone());
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
        // One-way, like `--no-mcp` below: off-only, so omitting it cannot
        // override a config file that already set `[vector] worker_enabled =
        // false`.
        if self.disable_vector_worker {
            cfg.vector.worker_enabled = false;
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
        if let Some(endpoint) = &self.otlp_endpoint {
            cfg.telemetry.endpoint = Some(endpoint.clone());
        }
        if let Some(protocol) = &self.otlp_protocol {
            cfg.telemetry.protocol = protocol.clone();
        }
        if let Some(ratio) = self.otlp_sample_ratio {
            cfg.telemetry.sample_ratio = ratio;
        }
        if let Some(name) = &self.otlp_service_name {
            cfg.telemetry.service_name = name.clone();
        }
        // One-way, like `--insecure-no-auth` above: the flag can only turn
        // names on, so omitting it cannot silently switch off what the config
        // file asked for. The direction matters more here than elsewhere — the
        // absent-flag default is the private one, so a bug in this branch
        // fails closed.
        if self.telemetry_include_names {
            cfg.telemetry.include_names = true;
        }
        Ok(())
    }
}

/// Parse the `KIMMY_OIDC_ROLE_MAPPINGS` value (ADR-078).
///
/// The error names the variable and shows the expected shape rather than
/// surfacing a bare serde dump, for the same reason every other refusal in
/// this file speaks in complete sentences: this is read out of an environment
/// block, where the person debugging it cannot see a stack trace either.
fn parse_role_mappings(raw: &str) -> Result<Vec<kimmy_auth::RoleMapping>> {
    const EXAMPLE: &str = "[{\"claim_value\":\"user\",\"grants\":[{\"db\":\"*\",\"actions\":[\"read\"]}]},\
                           {\"claim_value\":\"developer\",\"role\":\"analyst\"}]";
    serde_json::from_str(raw).map_err(|e| {
        anyhow::anyhow!(
            "invalid KIMMY_OIDC_ROLE_MAPPINGS: {e}\n\
             expected a JSON array of role mappings, each naming claim_value plus \
             role and/or grants, e.g.\n  {EXAMPLE}"
        )
    })
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// The workspace version is the single source of truth for both binaries
    /// (ADR-062). This pins the whole chain: `[workspace.package] version`
    /// is what this binary carries, and what it carries is the constant the
    /// startup log, `kimmyd --version` and `GET /v1/version` all print.
    #[test]
    fn the_server_version_is_the_workspace_version() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml");
        let manifest: toml::Value =
            toml::from_str(&std::fs::read_to_string(root).unwrap()).unwrap();
        let workspace = manifest["workspace"]["package"]["version"].as_str().unwrap();

        assert_eq!(workspace, env!("CARGO_PKG_VERSION"));
        assert_eq!(workspace, kimmy_core::build::VERSION);
    }

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("kimmyd").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("failed to parse {args:?}: {e}"))
    }

    #[test]
    fn restore_does_not_require_serving_configuration() {
        // It writes a file and exits. Requiring a root password would mean an
        // operator recovering from an incident has to invent one first.
        let cli = Cli::try_parse_from(["kimmyd", "restore", "--from", "/tmp/x.backup"]).unwrap();
        cli.resolve().expect("restore must resolve without auth configured");

        // Running still demands it.
        let cli = Cli::try_parse_from(["kimmyd", "run"]).unwrap();
        assert!(cli.resolve().is_err(), "serving without a root password must still be refused");
    }

    #[test]
    fn flags_override_defaults() {
        let cli = parse(&["--bind", "127.0.0.1:9999", "--data-dir", "/tmp/kimmy"]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg).unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:9999".parse().unwrap());
        assert_eq!(cfg.storage.data_dir, PathBuf::from("/tmp/kimmy"));
    }

    #[test]
    fn omitted_boolean_does_not_clear_the_config_file() {
        let cli = parse(&[]);
        let mut cfg = Config::default();
        cfg.auth.insecure_no_auth = true;
        cli.overrides.apply(&mut cfg).unwrap();
        assert!(cfg.auth.insecure_no_auth, "an absent flag must not override the file");
    }

    #[test]
    fn telemetry_flags_override_the_file_and_an_absent_one_does_not() {
        let cli = parse(&[
            "--otlp-endpoint",
            "http://collector:4318",
            "--otlp-protocol",
            "http/json",
            "--otlp-sample-ratio",
            "0.25",
            "--otlp-service-name",
            "kimmydb-prod",
        ]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg).unwrap();

        assert_eq!(cfg.telemetry.endpoint.as_deref(), Some("http://collector:4318"));
        assert_eq!(cfg.telemetry.protocol, "http/json");
        assert_eq!(cfg.telemetry.sample_ratio, 0.25);
        assert_eq!(cfg.telemetry.service_name, "kimmydb-prod");
        // Not passed, so untouched — and the untouched value is the private one.
        assert!(!cfg.telemetry.include_names);

        // The boolean is one-way. A file that asked for names must keep them
        // when the flag is absent, exactly as `--insecure-no-auth` behaves.
        let cli = parse(&[]);
        let mut cfg = Config::default();
        cfg.telemetry.include_names = true;
        cli.overrides.apply(&mut cfg).unwrap();
        assert!(cfg.telemetry.include_names, "an absent flag must not override the file");

        let cli = parse(&["--telemetry-include-names"]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg).unwrap();
        assert!(cfg.telemetry.include_names);
    }

    #[test]
    fn seeds_parse_and_imply_clustering() {
        let cli = parse(&["--seeds", "k8s:kimmy-headless.default.svc.cluster.local"]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg).unwrap();
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

    #[test]
    fn the_token_lifetime_limit_overrides_the_file_and_reaches_validate() {
        // The flag and the variable are one clap argument, so exercising the
        // flag exercises the variable's path; the variable's name is pinned
        // separately because it is documented and a rename would be a silent
        // break for every environment block that sets it.
        let arg = Cli::command()
            .get_arguments()
            .chain(Cli::command().get_subcommands().flat_map(|c| c.get_arguments()))
            .find(|a| a.get_id() == "oidc_max_token_lifetime_secs")
            .cloned()
            .expect("the argument exists");
        assert_eq!(
            arg.get_env().and_then(|e| e.to_str()),
            Some("KIMMY_OIDC_MAX_TOKEN_LIFETIME_SECS")
        );

        let cli = parse(&["--oidc-max-token-lifetime-secs", "3600"]);
        let mut cfg = Config::default();
        cfg.auth.oidc.max_token_lifetime_secs = 600;
        cli.overrides.apply(&mut cfg).unwrap();
        assert_eq!(cfg.auth.oidc.max_token_lifetime_secs, 3600, "the override wins");

        // Absent, the file's value stands — and the file's default is 900.
        let cli = parse(&[]);
        let mut cfg = Config::default();
        cli.overrides.apply(&mut cfg).unwrap();
        assert_eq!(cfg.auth.oidc.max_token_lifetime_secs, 900);

        // A value that arrived through the environment is refused by the same
        // rule as one in the file.
        let cli = parse(&["--oidc-max-token-lifetime-secs", "0"]);
        let mut cfg = Config {
            auth: crate::config::AuthConfig {
                root_password: Some("a-root-password-for-the-tests".into()),
                jwt_secret: Some("a-signing-key-of-adequate-length".into()),
                oidc: crate::config::OidcConfig {
                    issuer: Some("https://auth.example.com".into()),
                    audience: Some("https://kimmydb.example.com".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        cli.overrides.apply(&mut cfg).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("max_token_lifetime_secs"), "unhelpful error: {err}");
    }

    #[test]
    fn role_mappings_env_replaces_the_file_list() {
        let cli = parse(&[
            "--oidc-role-mappings",
            r#"[{"claim_value":"user","grants":[{"db":"*","actions":["read","search"]}]},
                {"claim_value":"developer","role":"analyst"}]"#,
        ]);
        let mut cfg = Config::default();
        // A file that already configured mappings must not survive alongside
        // the variable: "what I passed is what runs", like every other
        // override here.
        cfg.auth.oidc.role_mappings = vec![kimmy_auth::RoleMapping {
            claim_value: "from-the-file".into(),
            role: None,
            grants: vec![kimmy_auth::Grant::new("sales", "*", vec![kimmy_auth::Action::Read])],
        }];
        cli.overrides.apply(&mut cfg).unwrap();

        assert_eq!(cfg.auth.oidc.role_mappings.len(), 2, "the file's mapping must be replaced");
        assert_eq!(cfg.auth.oidc.role_mappings[0].claim_value, "user");
        assert_eq!(
            cfg.auth.oidc.role_mappings[0].grants[0].actions,
            vec![kimmy_auth::Action::Read, kimmy_auth::Action::Search],
            "lowercase action names are the wire form"
        );
        assert_eq!(cfg.auth.oidc.role_mappings[1].role.as_deref(), Some("analyst"));
    }

    #[test]
    fn an_unparsable_role_mappings_value_names_the_variable() {
        let cli = parse(&["--oidc-role-mappings", "[{claim_value: user}]"]);
        let mut cfg = Config::default();
        let err = cli.overrides.apply(&mut cfg).unwrap_err().to_string();
        assert!(err.contains("KIMMY_OIDC_ROLE_MAPPINGS"), "must name the variable: {err}");
        assert!(err.contains("claim_value"), "must show the expected shape: {err}");
    }

    #[test]
    fn role_mappings_from_the_env_reach_validate() {
        // The startup refusals are validate()'s to make, and the env form
        // lands in the same field the file does. Asserting the refusal itself
        // (not merely that something failed) is what keeps this test honest:
        // a mapping naming neither role nor grants is a typo however it
        // arrived.
        let cli = parse(&["--oidc-role-mappings", r#"[{"claim_value":"user"}]"#]);
        let mut cfg = Config {
            auth: crate::config::AuthConfig {
                root_password: Some("a-root-password-for-the-tests".into()),
                jwt_secret: Some("a-signing-key-of-adequate-length".into()),
                oidc: crate::config::OidcConfig {
                    issuer: Some("https://auth.example.com".into()),
                    audience: Some("https://kimmydb.example.com".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        cli.overrides.apply(&mut cfg).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("user"), "the error should name the mapping: {err}");
    }
}
