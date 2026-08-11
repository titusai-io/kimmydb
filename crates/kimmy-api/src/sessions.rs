//! Whether a token is still good, beyond its signature.
//!
//! A signature proves a token was issued by this cluster and has not expired.
//! It cannot prove the account still exists, is still enabled, or has not had
//! every one of its tokens invalidated since. That is what this checks
//! ([ADR-052](../../../docs/decisions.md)).
//!
//! # Why there is a cache at all
//!
//! The check needs the user's current record, and reading it per request would
//! put a storage read on a path that today does no I/O whatsoever. So each node
//! keeps a small map of user → state.
//!
//! # Two things evict from it, and neither is redundant
//!
//! A route that edits a user evicts **synchronously**, so a local
//! administrative action takes effect on this node whether or not any
//! background task is running — a single node is correct with no wiring at all.
//!
//! An oplog consumer evicts on any write to `__users`, which is the only way a
//! *replicated* edit can reach this node: `apply_remote` publishes on the node
//! that applied it exactly as a local write does. That is also the same shape
//! as the embedding worker and the webhook dispatcher.
//!
//! The split matters because it decides the failure mode. If the consumer were
//! the only mechanism, forgetting to spawn it would disable revocation
//! silently, which is precisely the class of bug this project keeps finding.
//! With both, a forgotten consumer delays *cluster-wide* revocation instead.
//!
//! # Two properties worth stating
//!
//! **A miss reads through.** This is a cache with the store as the authority,
//! not a replica of it. A map filled only by the log would be empty at startup
//! and would refuse every request until it caught up; populating on miss makes
//! a cold node slow for one request per user instead of wrong for all of them.
//!
//! **Falling behind clears it.** The broadcast channel is bounded, so a
//! consumer can be told it missed entries. A durable consumer would resume from
//! a position; a cache simply drops everything and re-reads, which cannot
//! silently serve a stale version.

use std::collections::HashMap;
use std::sync::Arc;

use kimmy_auth::{Principal, UserStore};
use kimmy_core::CollectionId;
use kimmy_storage::Engine;
use parking_lot::RwLock;
use tracing::{debug, warn};

use crate::error::ApiError;

/// What the cache remembers about a user.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct State {
    version: u64,
    disabled: bool,
}

/// Per-node view of which tokens are still honoured.
#[derive(Clone)]
pub struct Sessions {
    users: Arc<RwLock<HashMap<String, Option<State>>>>,
    store: Arc<UserStore>,
}

impl Sessions {
    pub fn new(store: UserStore) -> Self {
        Self { users: Arc::new(RwLock::new(HashMap::new())), store: Arc::new(store) }
    }

    /// Refuse a principal whose token no longer reflects its user.
    ///
    /// Three refusals, all of them 401 with the same message. They are not
    /// distinguished on purpose: telling a caller whether the account was
    /// deleted, disabled, or merely logged out reports on an account to
    /// whoever holds a stale token for it.
    pub fn check(&self, engine: &Engine, principal: &Principal) -> Result<(), ApiError> {
        // Authentication is off; there is no user record to check against.
        if principal.unauthenticated {
            return Ok(());
        }

        match self.state(engine, &principal.user) {
            // Gone. No record means no version to bump — the absence *is* the
            // revocation, which is how deleting a user ends its sessions.
            Ok(None) => Err(revoked()),
            Ok(Some(state)) if state.disabled => Err(revoked()),
            Ok(Some(state)) if state.version != principal.token_version => Err(revoked()),
            Ok(Some(_)) => Ok(()),
            // The store could not be read. Refusing is the strict direction,
            // and the honest one: a check that cannot run has not passed.
            Err(e) => {
                warn!(user = %principal.user, error = %e, "could not check the token version");
                Err(ApiError::unauthorized("could not verify this token"))
            }
        }
    }

    /// The user's state, from the cache or from the store.
    fn state(&self, engine: &Engine, user: &str) -> Result<Option<State>, String> {
        if let Some(cached) = self.users.read().get(user) {
            return Ok(*cached);
        }

        let state = self
            .store
            .get(engine, user)
            .map_err(|e| e.to_string())?
            .map(|u| State { version: u.token_version, disabled: u.disabled });

        // The absence is cached too: an unknown name must not cost a storage
        // read on every request, or an expired token becomes a way to make a
        // node do work. The consumer evicts it if that user is ever created.
        self.users.write().insert(user.to_string(), state);
        Ok(state)
    }

    /// Forget one user, so the next request re-reads.
    ///
    /// Called directly by the routes that edit a user, and by the oplog
    /// consumer. Both, deliberately: a *local* admin action must take effect
    /// whether or not the consumer is running, so a single node is correct
    /// with no background task at all, while a *replicated* edit can only
    /// arrive through the log. A forgotten consumer therefore delays
    /// cluster-wide revocation rather than silently disabling revocation.
    pub fn evict(&self, user: &str) {
        self.users.write().remove(user);
    }

    /// Forget everything.
    fn clear(&self) {
        self.users.write().clear();
    }

    #[cfg(test)]
    fn cached(&self, user: &str) -> Option<Option<State>> {
        self.users.read().get(user).copied()
    }
}

fn revoked() -> ApiError {
    ApiError::unauthorized("this token is no longer valid; log in again")
}

/// Evict cached user state as `__users` changes, forever.
///
/// Runs as its own task rather than inside the request path, because the point
/// is that a request does no I/O. A replicated edit reaches this the same way a
/// local one does — `apply_remote` publishes on the node that applied it — so
/// revoking on one node takes effect cluster-wide at replication speed with no
/// second transport.
/// Subscribing happens **here**, not inside the returned future, so the
/// caller's `tokio::spawn` cannot miss an entry published between the call and
/// the task being polled for the first time.
pub fn invalidator(
    engine: &Engine,
    sessions: Sessions,
) -> impl std::future::Future<Output = ()> + use<> {
    // Derived from the names, so this is the same id on every node — the same
    // property that lets a replicated entry address the same collection
    // everywhere.
    let users = CollectionId::derive(kimmy_auth::SYSTEM_DB, kimmy_auth::USERS_COLLECTION);
    let mut events = engine.subscribe();

    async move {
        loop {
            match events.recv().await {
                Ok(entry) => {
                    if entry.collection != users {
                        continue;
                    }
                    match entry.doc_id.as_ref() {
                        Some(id) => {
                            let user = id.to_string();
                            debug!(%user, "user record changed; dropping cached token state");
                            sessions.evict(&user);
                        }
                        // An entry against the collection naming no document is
                        // not something this can localise, so it takes the safe
                        // interpretation.
                        None => sessions.clear(),
                    }
                }
                // Missed entries: which users changed is unknown, so nothing
                // cached can be trusted. Dropping it all costs a re-read.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(missed = n, "token-state consumer fell behind; clearing the cache");
                    sessions.clear();
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use kimmy_auth::Grant;

    use super::*;

    fn engine() -> (Arc<Engine>, Sessions, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let store = UserStore::open(&engine).unwrap();
        (Arc::clone(&engine), Sessions::new(store), dir)
    }

    fn user(name: &str) -> Principal {
        Principal::new(name, vec![Grant::superuser()])
    }

    #[test]
    fn a_token_at_the_current_version_is_honoured() {
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();

        assert!(sessions.check(&engine, &user("ada").at_version(0)).is_ok());
    }

    #[test]
    fn a_token_from_before_a_bump_is_refused() {
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();

        // The token was issued at version 0.
        let held = user("ada").at_version(0);
        assert!(sessions.check(&engine, &held).is_ok());

        store.set_password(&engine, "ada", "another-password").unwrap();
        sessions.evict("ada"); // what the consumer does on the oplog entry

        assert!(
            sessions.check(&engine, &held).is_err(),
            "a password change must end sessions the old one opened"
        );
    }

    #[test]
    fn changing_grants_ends_the_sessions_that_hold_the_old_ones() {
        // The quiet win: grants ride inside the token, so without this a
        // narrowed permission would keep working until the token expired.
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();
        let held = user("ada").at_version(0);

        store.set_grants(&engine, "ada", vec![]).unwrap();
        sessions.evict("ada");

        assert!(sessions.check(&engine, &held).is_err());
    }

    #[test]
    fn a_deleted_users_token_is_refused_without_any_bump() {
        // There is no record left to hold a version: the absence is the
        // revocation. This is the debt the task existed to close.
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();
        store.create(&engine, "root", "password123", vec![Grant::superuser()]).unwrap();
        let held = user("ada").at_version(0);
        assert!(sessions.check(&engine, &held).is_ok());

        store.delete(&engine, "ada").unwrap();
        sessions.evict("ada");

        assert!(sessions.check(&engine, &held).is_err(), "a deleted account must stop working");
    }

    #[test]
    fn a_disabled_users_token_is_refused() {
        // `disabled` existed already and was checked at login and nowhere
        // afterwards, so disabling an account left its live tokens working.
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        let mut u = store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();
        let held = user("ada").at_version(0);
        assert!(sessions.check(&engine, &held).is_ok());

        u.disabled = true;
        store.replace_for_test(&engine, &u).unwrap();
        sessions.evict("ada");

        assert!(sessions.check(&engine, &held).is_err());
    }

    #[test]
    fn an_unknown_user_is_cached_so_it_costs_one_read_not_one_per_request() {
        // Otherwise an expired-account token is a way to make a node do a
        // storage read per request.
        let (engine, sessions, _dir) = engine();
        assert!(sessions.check(&engine, &user("nobody").at_version(0)).is_err());
        assert_eq!(sessions.cached("nobody"), Some(None), "the absence must be remembered");
    }

    #[test]
    fn clearing_the_cache_makes_the_next_check_re_read() {
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();

        assert!(sessions.check(&engine, &user("ada").at_version(0)).is_ok());
        assert!(sessions.cached("ada").is_some());

        sessions.clear();
        assert_eq!(sessions.cached("ada"), None, "a lagged consumer drops everything");
        // ...and the answer is still right, because the store is the authority.
        assert!(sessions.check(&engine, &user("ada").at_version(0)).is_ok());
    }

    #[test]
    fn authentication_being_off_skips_the_check_entirely() {
        // There is no user record behind `--insecure-no-auth`, so a lookup
        // would refuse every request on a server that has no users at all.
        let (engine, sessions, _dir) = engine();
        assert!(sessions.check(&engine, &Principal::insecure_root()).is_ok());
    }

    #[tokio::test]
    async fn the_consumer_evicts_on_a_real_oplog_entry() {
        // The wiring, not the policy: a write to `__users` must actually reach
        // the cache through the broadcast channel.
        let (engine, sessions, _dir) = engine();
        let store = UserStore::open(&engine).unwrap();
        store.create(&engine, "ada", "password123", vec![Grant::superuser()]).unwrap();

        let task = tokio::spawn(invalidator(&engine, sessions.clone()));
        assert!(sessions.check(&engine, &user("ada").at_version(0)).is_ok());
        assert!(sessions.cached("ada").is_some(), "the check populated the cache");

        store.set_password(&engine, "ada", "another-password").unwrap();

        // No sleep: poll until the consumer has run, with a deadline.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sessions.cached("ada").is_some() {
            assert!(std::time::Instant::now() < deadline, "the consumer never evicted");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            sessions.check(&engine, &user("ada").at_version(0)).is_err(),
            "and the re-read must see the bumped version"
        );
        task.abort();
    }
}
