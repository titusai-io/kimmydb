//! Node lifecycle: startup, serving, and graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kimmy_auth::{TokenIssuer, UserStore};
use kimmy_storage::{Engine, RetentionPolicy};
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

    let state = kimmy_api::state(Arc::clone(&engine), tokens, config.auth.insecure_no_auth)
        .context("building the API state")?;

    // MCP shares the state rather than being handed its own, so an agent tool
    // and the REST route beside it reach the same engine through the same
    // authorization check. See kimmy-mcp's crate documentation.
    let mut app = kimmy_api::router(Arc::clone(&state));
    if config.server.mcp {
        app = app.merge(kimmy_mcp::mcp_router(
            Arc::clone(&state),
            config.server.mcp_allowed_hosts.clone(),
        ));
        info!("serving MCP at /mcp");
    }

    let gc_handle = spawn_collector(Arc::clone(&engine), &config);
    let cluster_handles = spawn_cluster(Arc::clone(&engine), &config).await?;

    // The embedding worker is an ordinary change-stream subscriber, so it runs
    // alongside the server rather than inside the write path. A write returns
    // as soon as its oplog entry is durable; embedding catches up behind it.
    let worker_handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            let mut worker = kimmy_vector::EmbeddingWorker::new(engine);
            if let Err(e) = worker.run().await {
                warn!(error = %e, "embedding worker stopped");
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    let local = listener.local_addr().unwrap_or(config.server.bind);
    info!(bind = %local, "serving HTTP and WebSocket");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving")?;

    // The worker holds no locks and its position is durable, so aborting is
    // safe: whatever it had not finished is re-delivered on the next start.
    worker_handle.abort();
    // Likewise the collector: a pass is a transaction, so an aborted one either
    // committed or did not, and the next start simply finds the same garbage.
    if let Some(handle) = gc_handle {
        handle.abort();
    }
    // And replication: anti-entropy is idempotent and resumes from version
    // vectors, so an interrupted round costs nothing but a repeat.
    for handle in cluster_handles {
        handle.abort();
    }

    info!("shutdown complete");
    Ok(())
}

/// Start the replication listener and peer loop, unless clustering is off.
///
/// Binding happens here rather than inside the task so that a port already in
/// use is a startup failure with a clear message, not a warning in a log nobody
/// reads while the node silently never replicates.
async fn spawn_cluster(
    engine: Arc<Engine>,
    config: &Config,
) -> Result<Vec<tokio::task::JoinHandle<()>>> {
    if !config.cluster.enabled {
        return Ok(Vec::new());
    }

    let secret = config.cluster.cluster_secret.clone().context(
        "cluster.enabled is set with no cluster_secret (validation should have caught this)",
    )?;

    let listener = tokio::net::TcpListener::bind(config.cluster.bind)
        .await
        .with_context(|| format!("binding the cluster listener on {}", config.cluster.bind))?;
    let local = listener.local_addr().unwrap_or(config.cluster.bind);

    let serving = tokio::spawn(kimmy_cluster::serve(Arc::clone(&engine), listener, secret.clone()));

    let replicating = tokio::spawn(kimmy_cluster::replicate(
        engine,
        kimmy_cluster::ReplicationConfig {
            seeds: config.cluster.seeds.clone(),
            secret,
            local,
            sync_interval: Duration::from_secs(config.cluster.sync_interval_secs),
            discovery_interval: Duration::from_secs(config.cluster.discovery_interval_secs),
        },
    ));

    info!(
        bind = %local,
        seeds = config.cluster.seeds.len(),
        "clustering enabled"
    );
    Ok(vec![serving, replicating])
}

/// Start the retention collector, unless it is disabled.
fn spawn_collector(engine: Arc<Engine>, config: &Config) -> Option<tokio::task::JoinHandle<()>> {
    if config.storage.gc_interval_secs == 0 {
        warn!("retention collection is disabled; the oplog and tombstones will grow without bound");
        return None;
    }

    let interval = Duration::from_secs(config.storage.gc_interval_secs);
    let policy = RetentionPolicy::new(
        config.storage.oplog_retention_secs,
        config.storage.tombstone_retention_secs,
    );

    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately, which would collect during startup
        // while the node is still opening for business. Skip it.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            match engine.collect_garbage(policy) {
                // A failed pass is not fatal — the garbage is still there and
                // the next tick will find it — so it is logged and retried
                // rather than taking the node down.
                Err(e) => warn!(error = %e, "retention pass failed"),
                Ok(outcome) if outcome.is_empty() => {}
                Ok(outcome) => info!(
                    oplog = outcome.oplog_removed,
                    tombstones = outcome.tombstones_removed,
                    "collected expired records"
                ),
            }
        }
    }))
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
