//! `kimmyd` — the KimmyDB server.

mod cli;
mod config;
mod logging;
mod node;

use anyhow::{Context, Result};
use clap::Parser;

use crate::cli::{Cli, Command};

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Resolve and validate configuration before starting the runtime, so a
    // misconfiguration is a clean one-line error rather than a panic buried in
    // a worker thread.
    let config = cli.resolve()?;
    logging::init(&config.log)?;

    match cli.command.unwrap_or(Command::Run) {
        Command::CheckConfig => {
            println!("{}", toml::to_string_pretty(&config)?);
            eprintln!("configuration is valid");
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
            runtime.block_on(node::run(config))
        }
    }
}
