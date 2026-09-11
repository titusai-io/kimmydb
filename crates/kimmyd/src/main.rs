//! `kimmyd` — the KimmyDB server.

mod cli;
mod config;
mod lifecycle;
mod logging;
mod node;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::{Cli, Command};

// The global allocator, set in the binary and nowhere else. The release
// binary is a static musl build on every channel, and musl's malloc takes one
// lock per allocation: at eight concurrent clients a paged `find` ran
// thirteen times slower than the same code against glibc. mimalloc recovers
// it on every target alike (ADR-117; the table is in docs/benchmarks.md).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Resolve and validate configuration before starting the runtime, so a
    // misconfiguration is a clean one-line error rather than a panic buried in
    // a worker thread.
    let config = cli.resolve()?;

    // The command is decided *before* logging is installed, which it did not
    // used to be. Telemetry is only handed to the subscriber for `run`:
    // `check-config` and `restore` neither serve nor last more than a moment,
    // and starting an exporter for them would mean validating a file opens a
    // connection to production's collector.
    let command = cli.command.unwrap_or(Command::Run);
    let telemetry = matches!(command, Command::Run).then_some(&config.telemetry);
    let telemetry_guard = logging::init(&config.log, telemetry)?;

    match command {
        Command::CheckConfig => {
            println!("{}", toml::to_string_pretty(&config)?);
            eprintln!("configuration is valid");

            // Said in words as well as printed as TOML above, because this is
            // the setting whose effect — a 403 or 404 from the login route —
            // is met somewhere other than where it was configured. Only when
            // it is not the default: a default printed every time is a
            // default nobody reads.
            match config.auth.local.login_mode()? {
                kimmy_api::LocalLogin::Always => {}
                kimmy_api::LocalLogin::LoopbackOnly => eprintln!(
                    "local login is restricted to loopback connections: `kimmy login <user>` \
                     works from this host only, judged by the TCP peer (a reverse proxy on \
                     this host will look like loopback). Tokens already issued keep working."
                ),
                kimmy_api::LocalLogin::Disabled => eprintln!(
                    "local login is disabled: /v1/auth/login answers 404 and only the identity \
                     provider can authenticate a caller. Tokens already issued keep working."
                ),
            }

            // The one check that needs the network, and the only place it is
            // ever fatal. The server retries instead of refusing, because a
            // briefly unreachable identity provider must not stop a database
            // from restarting — which means a misspelled issuer would
            // otherwise show up as a warning in a log and federated tokens
            // failing forever. Here, an operator is watching.
            if config.auth.oidc.is_configured() {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("building the tokio runtime")?;
                let issuer = config.auth.oidc.issuer.as_deref().unwrap_or("");
                // Said here as well as at startup, because this is where an
                // operator comes to find out why a client cannot discover them.
                match config.auth.oidc.settings().and_then(|s| {
                    s.resource_identifier().map(|r| (r.to_string(), s.audience.clone()))
                }) {
                    Some((resource, _)) => eprintln!(
                        "this node publishes protected resource metadata for {resource} at \
                         {path}, so `kimmy login --url ...` needs no other configuration",
                        path = kimmy_auth::PROTECTED_RESOURCE_METADATA_PATH
                    ),
                    None => eprintln!(
                        "note: auth.oidc.audience is not an https URI, so no protected resource \
                         metadata is published and a client cannot ask for a token scoped to \
                         this node. Tokens will carry whatever audience the provider defaults \
                         to, shared with every other resource that trusts it."
                    ),
                }
                match runtime.block_on(node::probe_oidc(&config.auth.oidc)) {
                    Ok(keys) => eprintln!(
                        "identity provider {issuer} answered discovery and published {keys} \
                         signing key(s)"
                    ),
                    Err(e) => {
                        eprintln!("identity provider {issuer} could not be reached: {e:#}");
                        eprintln!(
                            "note: the server does NOT refuse to start for this -- it retries in \
                             the background, and local users keep working meanwhile."
                        );
                        return Err(e.context("checking the OIDC provider"));
                    }
                }
            }
            Ok(())
        }
        Command::Restore { from, until } => {
            // Into the configured data directory, at the same filename a node
            // opens, so that starting the node afterwards needs no extra flag.
            let target = config.storage.data_dir.join("kimmy.redb");
            std::fs::create_dir_all(&config.storage.data_dir).with_context(|| {
                format!("creating data directory {}", config.storage.data_dir.display())
            })?;
            let mut file = std::fs::File::open(&from)
                .with_context(|| format!("opening the backup {}", from.display()))?;

            let info = kimmy_storage::backup::restore(&target, &mut file)
                .with_context(|| format!("restoring into {}", target.display()))?;

            eprintln!(
                "restored {} records into {} (node {}, backup taken at {})",
                info.records,
                target.display(),
                info.node.map(|n| n.to_string()).unwrap_or_else(|| "unknown".into()),
                info.created_ms,
            );
            if let Some(until_ms) = until {
                // Opened only after the restore has written the file, because
                // the rewind reads the oplog the restore just put there.
                let engine = kimmy_storage::Engine::open(&target)
                    .with_context(|| format!("opening {} to rewind it", target.display()))?;
                let outcome = engine
                    .rewind_to(kimmy_core::Hlc::new(until_ms, 0))
                    .context("rewinding to the requested point in time")?;
                eprintln!(
                    "rewound to {until_ms}: {} document(s) reverted, {} removed, {} oplog \
                     entries discarded",
                    outcome.reverted, outcome.removed, outcome.oplog_discarded,
                );
                eprintln!(
                    "note: a rewound database has had history removed. Do not let it rejoin a \
                     cluster that still holds the undone writes -- anti-entropy would put them \
                     back."
                );
            }

            // A restored directory has a database and no run behind it. The
            // marker says so, or the first start would warn about a run that
            // did not shut down cleanly when there was no run at all.
            lifecycle::record_exit(&config.storage.data_dir, lifecycle::Exit::Restore);

            eprintln!(
                "note: this database carries the original node's identity. Do not start it \
                 alongside the node it was taken from."
            );
            Ok(())
        }
        Command::Run => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            let outcome = runtime.block_on(node::run(config));
            // Dropped *after* `node::run` returns, which is after every
            // background task has been aborted — the same shutdown discipline
            // the end of `node::run` already follows, for the same reason: a
            // batch processor buffers, so shutting the exporter down first
            // would throw away the last few seconds of spans, which is exactly
            // the part anybody debugging a shutdown wants to see.
            drop(telemetry_guard);
            outcome
        }
    }
}
