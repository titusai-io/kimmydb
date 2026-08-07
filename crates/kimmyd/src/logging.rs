//! Tracing subscriber setup.

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::config::{LogConfig, LogFormat};

/// Install the global tracing subscriber.
///
/// `RUST_LOG` takes precedence when set, since that is what an operator will
/// reach for when debugging a running container.
pub fn init(cfg: &LogConfig) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .with_context(|| format!("invalid log filter {:?}", cfg.level))?;

    let registry = tracing_subscriber::registry().with(filter);
    match cfg.format {
        LogFormat::Pretty => {
            registry.with(tracing_subscriber::fmt::layer().with_target(true)).init();
        }
        LogFormat::Json => {
            registry.with(tracing_subscriber::fmt::layer().json().with_target(true)).init();
        }
    }
    Ok(())
}
