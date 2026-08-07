//! Node lifecycle: startup, serving, and graceful shutdown.

use std::sync::Arc;

use anyhow::{Context, Result};
use kimmy_auth::{TokenIssuer, UserStore};
use kimmy_storage::Engine;
use tracing::{info, warn};

use crate::config::Config;

/// Filename of the redb database inside the data directory.
const DATABASE_FILE: &str = "kimmy.redb";

pub async fn run(config: Config) -> Result<()> {
    std::fs::create_dir_all(&config.storage.data_dir).with_context(|| {
        format!("creating data directory {}", config.storage.data_dir.display())
    })?;

    // Node identity lives inside the database file rather than beside it, so
    // that copying or restoring the file carries the identity with it. That
    // matters because the id is the tiebreak half of every write's stamp — a
    // node that forgets it becomes a stranger to its own prior writes.
    let path = config.storage.data_dir.join(DATABASE_FILE);
    let engine = Arc::new(
        Engine::open(&path).with_context(|| format!("opening database {}", path.display()))?,
    );

    info!(node = %engine.node_id(), version = env!("CARGO_PKG_VERSION"), "starting kimmyd");
    info!("{}", config.summary());

    if config.auth.insecure_no_auth {
        warn!("authentication is DISABLED; every request runs with full privileges");
    } else {
        bootstrap_users(&engine, &config)?;
    }

    // With auth off, no token is ever verified, so a throwaway signing key is
    // correct rather than a placeholder that might be mistaken for a secret.
    let secret = config
        .auth
        .jwt_secret
        .clone()
        .unwrap_or_else(|| "insecure-no-auth-unused-signing-key".to_string());
    let tokens = TokenIssuer::new(&secret, config.auth.token_ttl_secs)
        .context("configuring the token issuer")?;

    let app = kimmy_api::build(Arc::clone(&engine), tokens, config.auth.insecure_no_auth)
        .context("building the API router")?;

    let listener = tokio::net::TcpListener::bind(config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    let local = listener.local_addr().unwrap_or(config.server.bind);
    info!(bind = %local, "serving HTTP and WebSocket");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving")?;

    info!("shutdown complete");
    Ok(())
}

/// Create the bootstrap superuser on first start.
fn bootstrap_users(engine: &Engine, config: &Config) -> Result<()> {
    let store = UserStore::open(engine).context("opening the user store")?;
    let password = config
        .auth
        .root_password
        .as_deref()
        .context("no root password configured (validation should have caught this)")?;

    if store.bootstrap_root(engine, &config.auth.root_user, password)? {
        info!(user = %config.auth.root_user, "bootstrapped the superuser");
    }
    Ok(())
}

/// Resolve on SIGINT or SIGTERM. SIGTERM matters most — it is what Docker and
/// Kubernetes send, and ignoring it means a hard kill after the grace period.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "could not install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutdown signal received, draining");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_identity_is_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DATABASE_FILE);

        let first = Engine::open(&path).unwrap().node_id();
        let second = Engine::open(&path).unwrap().node_id();
        assert_eq!(first, second, "identity must survive a restart");
    }
}
