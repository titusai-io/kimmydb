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
        Command::Run => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            runtime.block_on(node::run(config))
        }
    }
}
