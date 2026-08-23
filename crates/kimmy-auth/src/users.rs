//! The persistent user store.
//!
//! Users live in an ordinary collection in a reserved system database, so they
//! get the same durability, oplog, and (eventually) replication as any other
//! data. The `__` name prefix is rejected for user-created objects precisely so
//! that nothing can collide with these.

use kimmy_core::DocId;
use kimmy_storage::{CollectionMeta, Engine};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::error::{AuthError, Result};
use crate::password;
use crate::rbac::{Grant, Principal};
use crate::roles::RoleStore;

/// Reserved database holding server metadata.
pub const SYSTEM_DB: &str = "__kimmy";
/// Collection holding user records.
pub const USERS_COLLECTION: &str = "__users";

/// A stored user.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "_id")]
    pub name: String,
    /// Argon2id PHC string. Never leaves this crate.
    pub password_hash: String,
    #[serde(default)]
    pub grants: Vec<Grant>,
    /// Named roles this user holds, resolved through the role store.
    ///
    /// Additive with `grants`, never a replacement: effective permission is the
    /// union of the two (ADR-073). `default` is what lets every user record
    /// written before roles existed decode as holding none, which is why this
    /// needed no migration.
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub disabled: bool,
    /// Bumped to invalidate every token this user currently holds.
    ///
    /// A token carries the value it was issued under, so a mismatch is a
    /// refusal. `default` matters: users stored before this field existed
    /// decode as 0, which is what their tokens also claim (ADR-052).
    #[serde(default)]
    pub token_version: u64,
}

impl User {
    /// The principal this user authorizes, given the grants its roles resolve to.
    ///
    /// Role grants and direct grants are a **union**, always (ADR-073).
    /// Kubernetes RBAC is purely additive and Postgres unions privileges across
    /// role membership; more to the point, a union is the only rule that needs
    /// no rewrite of an existing user record, since a user holding no roles
    /// gets exactly what it got before.
    fn to_principal(&self, role_grants: Vec<Grant>) -> Principal {
        let mut grants = self.grants.clone();
        grants.extend(role_grants);
        Principal::new(self.name.clone(), grants).at_version(self.token_version)
    }
}

/// Reads and writes users against the storage engine.
pub struct UserStore {
    collection: CollectionMeta,
    /// Held so that a user's effective grants can be resolved without every
    /// call site having to know that roles exist.
    roles: RoleStore,
}

impl UserStore {
    /// Open the store, creating the system collection if needed.
    pub fn open(engine: &Engine) -> Result<Self> {
        let collection = engine
            .create_system_collection(SYSTEM_DB, USERS_COLLECTION)
            .map_err(|e| AuthError::Hashing(format!("opening the user store: {e}")))?;
        Ok(Self { collection, roles: RoleStore::open(engine)? })
    }

    /// The role store this user store resolves names against.
    pub fn roles(&self) -> &RoleStore {
        &self.roles
    }

    /// Everything a user is permitted, direct grants and role grants together.
    pub fn effective_grants(&self, engine: &Engine, user: &User) -> Result<Vec<Grant>> {
        let mut grants = user.grants.clone();
        grants.extend(self.roles.grants_for(engine, &user.roles)?);
        Ok(grants)
    }

    /// Replace the roles a user holds, taking effect immediately.
    ///
    /// Bumps `token_version` for the same reason [`Self::set_grants`] does: the
    /// grants a role resolves to are embedded in the token at login, so without
    /// the bump a *narrowing* edit would do nothing until the token expired.
    pub fn set_roles(&self, engine: &Engine, name: &str, roles: Vec<String>) -> Result<()> {
        let mut user =
            self.get(engine, name)?.ok_or_else(|| AuthError::UserNotFound(name.into()))?;
        user.roles = roles;
        user.token_version = user.token_version.wrapping_add(1);
        self.put(engine, &user)
    }

    /// Invalidate every token held by a user carrying `role`, returning how many.
    ///
    /// **This is what keeps a role edit honest, and it is the easiest thing in
    /// the feature to leave out.** A local user's grants are resolved at login
    /// and embedded in its token, so narrowing a role changes nothing for
    /// anyone already holding one until it expires — silently contradicting the
    /// promise [`Self::set_grants`] has made since ADR-052.
    ///
    /// A scan, because users are documents in a collection and there is no
    /// index from role to holder. That is the honest cost of the storage shape;
    /// a role edit is an administrative action, not a request-path one.
    ///
    /// Federated principals need nothing here and get nothing: they have no
    /// user record and no token version (ADR-065), and their grants are
    /// resolved from the mapping on every request, so a role edit already
    /// applies to them immediately.
    pub fn invalidate_holders_of_role(&self, engine: &Engine, role: &str) -> Result<u64> {
        let mut holders = Vec::new();
        for name in self.list(engine)? {
            if let Some(user) = self.get(engine, &name)?
                && user.roles.iter().any(|held| held == role)
            {
                holders.push(user);
            }
        }

        let count = holders.len() as u64;
        for mut user in holders {
            user.token_version = user.token_version.wrapping_add(1);
            self.put(engine, &user)?;
        }
        Ok(count)
    }

    /// Create the bootstrap superuser if the store is empty.
    ///
    /// Only on an empty store: re-running the server with a different
    /// `KIMMY_ROOT_PASSWORD` must not silently reset an existing account, which
    /// would turn a stale environment variable into a privilege grant.
    pub fn bootstrap_root(&self, engine: &Engine, name: &str, password: &str) -> Result<bool> {
        if self.count(engine)? > 0 {
            return Ok(false);
        }
        self.create(engine, name, password, vec![Grant::superuser()])?;
        info!(user = name, "created the bootstrap superuser");
        Ok(true)
    }

    pub fn count(&self, engine: &Engine) -> Result<u64> {
        engine.count(&self.collection).map_err(storage_error)
    }

    pub fn create(
        &self,
        engine: &Engine,
        name: &str,
        password: &str,
        grants: Vec<Grant>,
    ) -> Result<User> {
        if self.get(engine, name)?.is_some() {
            return Err(AuthError::UserExists(name.to_string()));
        }
        let user = User {
            name: name.to_string(),
            password_hash: password::hash(password)?,
            grants,
            roles: Vec::new(),
            disabled: false,
            token_version: 0,
        };
        let doc = bson::serialize_to_document(&user)
            .map_err(|e| AuthError::Hashing(format!("encoding user: {e}")))?;
        engine.insert(&self.collection, doc).map_err(storage_error)?;
        Ok(user)
    }

    pub fn get(&self, engine: &Engine, name: &str) -> Result<Option<User>> {
        let id = DocId::String(name.to_string());
        let Some(doc) = engine.get(&self.collection, &id).map_err(storage_error)? else {
            return Ok(None);
        };
        bson::deserialize_from_document(doc)
            .map(Some)
            .map_err(|e| AuthError::Hashing(format!("decoding user {name:?}: {e}")))
    }

    pub fn list(&self, engine: &Engine) -> Result<Vec<String>> {
        let mut names = Vec::new();
        engine
            .for_each_doc(&self.collection, |id, _| {
                names.push(id.to_string());
                Ok(true)
            })
            .map_err(storage_error)?;
        Ok(names)
    }

    /// Store a user record as given.
    ///
    /// For tests that need a state no setter produces on its own — a disabled
    /// account, most usefully, since nothing else writes that flag yet.
    pub fn replace_for_test(&self, engine: &Engine, user: &User) -> Result<()> {
        self.put(engine, user)
    }

    pub fn delete(&self, engine: &Engine, name: &str) -> Result<bool> {
        let id = DocId::String(name.to_string());
        engine.delete(&self.collection, &id).map_err(storage_error)
    }

    /// Set a new password, ending every session the old one opened.
    ///
    /// The bump is the conventional behaviour and the one someone expects
    /// after a suspected compromise: changing the password logs out everyone
    /// holding a token for this account, including whoever took it.
    pub fn set_password(&self, engine: &Engine, name: &str, password: &str) -> Result<()> {
        let mut user =
            self.get(engine, name)?.ok_or_else(|| AuthError::UserNotFound(name.into()))?;
        user.password_hash = password::hash(password)?;
        user.token_version = user.token_version.wrapping_add(1);
        self.put(engine, &user)
    }

    /// Replace a user's grants, taking effect immediately.
    ///
    /// Grants are embedded in the token, so without the bump an edit — a
    /// *narrowing* one especially — would do nothing until the token expired.
    /// The cost is that there is no refresh flow, so this logs the user out.
    pub fn set_grants(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<()> {
        let mut user =
            self.get(engine, name)?.ok_or_else(|| AuthError::UserNotFound(name.into()))?;
        user.grants = grants;
        user.token_version = user.token_version.wrapping_add(1);
        self.put(engine, &user)
    }

    fn put(&self, engine: &Engine, user: &User) -> Result<()> {
        let doc = bson::serialize_to_document(user)
            .map_err(|e| AuthError::Hashing(format!("encoding user: {e}")))?;
        let id = DocId::String(user.name.clone());
        engine.replace(&self.collection, &id, doc, true).map_err(storage_error)?;
        Ok(())
    }

    /// Verify credentials and return the principal they authorize.
    ///
    /// Every failure returns the same error. Distinguishing "no such user" from
    /// "wrong password" turns the login endpoint into a user enumeration oracle.
    pub fn authenticate(&self, engine: &Engine, name: &str, password: &str) -> Result<Principal> {
        let user = self.get(engine, name)?;

        let Some(user) = user else {
            // Hash anyway, so a missing user costs the same time as a wrong
            // password and the difference is not observable by timing.
            let _ = password::verify(password, DUMMY_HASH);
            return Err(AuthError::InvalidCredentials);
        };

        if !password::verify(password, &user.password_hash) {
            return Err(AuthError::InvalidCredentials);
        }
        if user.disabled {
            warn!(user = name, "rejected login for a disabled account");
            return Err(AuthError::InvalidCredentials);
        }

        let role_grants = self.roles.grants_for(engine, &user.roles)?;
        Ok(user.to_principal(role_grants))
    }
}

/// A real Argon2id hash, used to equalize timing when a user does not exist.
/// The plaintext is irrelevant; only the work factor matters.
const DUMMY_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzb21lc2FsdA$\
    JbQnG7z1PbTsn0k7WT0LvJKKVQmJVBEcnRPBrIsCTFE";

fn storage_error(e: kimmy_storage::StorageError) -> AuthError {
    AuthError::Hashing(format!("user store: {e}"))
}

#[cfg(test)]
mod tests {
    use kimmy_storage::Engine;

    use super::*;
    use crate::rbac::Action;

    fn setup() -> (Engine, UserStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let store = UserStore::open(&engine).unwrap();
        (engine, store, dir)
    }

    #[test]
    fn a_created_user_can_authenticate() {
        let (engine, store, _dir) = setup();
        store
            .create(&engine, "ada", "hunter2", vec![Grant::new("db", "*", vec![Action::Read])])
            .unwrap();

        let principal = store.authenticate(&engine, "ada", "hunter2").unwrap();
        assert_eq!(principal.user, "ada");
        assert!(principal.can(Action::Read, "db", Some("c")));
    }

    #[test]
    fn a_wrong_password_is_rejected() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "hunter2", vec![]).unwrap();
        assert!(matches!(
            store.authenticate(&engine, "ada", "wrong"),
            Err(AuthError::InvalidCredentials)
        ));
    }

    #[test]
    fn a_missing_user_yields_the_same_error_as_a_wrong_password() {
        // Distinguishing them would turn login into a user-enumeration oracle.
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "hunter2", vec![]).unwrap();

        let wrong_password = store.authenticate(&engine, "ada", "nope").unwrap_err();
        let no_such_user = store.authenticate(&engine, "nobody", "nope").unwrap_err();
        assert_eq!(wrong_password.to_string(), no_such_user.to_string());
    }

    #[test]
    fn the_stored_record_never_holds_the_plaintext() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "swordfish", vec![]).unwrap();
        let user = store.get(&engine, "ada").unwrap().unwrap();
        assert!(!user.password_hash.contains("swordfish"));
        assert!(user.password_hash.starts_with("$argon2id$"));
    }

    #[test]
    fn duplicate_users_are_rejected() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "a", vec![]).unwrap();
        assert!(matches!(store.create(&engine, "ada", "b", vec![]), Err(AuthError::UserExists(_))));
        // The original password must still work after the rejected create.
        assert!(store.authenticate(&engine, "ada", "a").is_ok());
    }

    #[test]
    fn a_disabled_account_cannot_log_in() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "pw", vec![]).unwrap();
        let mut user = store.get(&engine, "ada").unwrap().unwrap();
        user.disabled = true;
        store.put(&engine, &user).unwrap();

        assert!(store.authenticate(&engine, "ada", "pw").is_err());
    }

    #[test]
    fn passwords_and_grants_can_be_changed() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "old", vec![]).unwrap();

        store.set_password(&engine, "ada", "new").unwrap();
        assert!(store.authenticate(&engine, "ada", "old").is_err());
        assert!(store.authenticate(&engine, "ada", "new").is_ok());

        store.set_grants(&engine, "ada", vec![Grant::new("db", "*", vec![Action::Write])]).unwrap();
        let principal = store.authenticate(&engine, "ada", "new").unwrap();
        assert!(principal.can(Action::Write, "db", Some("c")));
    }

    #[test]
    fn deleting_a_user_revokes_access() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "pw", vec![]).unwrap();
        assert!(store.delete(&engine, "ada").unwrap());
        assert!(store.authenticate(&engine, "ada", "pw").is_err());
        assert!(!store.delete(&engine, "ada").unwrap());
    }

    #[test]
    fn bootstrap_creates_a_superuser_only_when_the_store_is_empty() {
        let (engine, store, _dir) = setup();
        assert!(store.bootstrap_root(&engine, "root", "first").unwrap());

        let principal = store.authenticate(&engine, "root", "first").unwrap();
        assert!(principal.can(Action::Admin, "anything", None));

        // A second start with a different password must not reset the account,
        // or a stale environment variable becomes a privilege grant.
        assert!(!store.bootstrap_root(&engine, "root", "second").unwrap());
        assert!(store.authenticate(&engine, "root", "second").is_err());
        assert!(store.authenticate(&engine, "root", "first").is_ok());
    }

    #[test]
    fn users_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let store = UserStore::open(&engine).unwrap();
            store.create(&engine, "ada", "pw", vec![Grant::superuser()]).unwrap();
        }

        let engine = Engine::open(&path).unwrap();
        let store = UserStore::open(&engine).unwrap();
        assert!(store.authenticate(&engine, "ada", "pw").is_ok());
        assert_eq!(store.list(&engine).unwrap(), vec!["ada"]);
    }

    #[test]
    fn the_system_collection_cannot_be_created_through_the_user_facing_path() {
        // The `__` prefix is reserved so nothing can shadow the user store.
        let (engine, _store, _dir) = setup();
        assert!(engine.create_collection(SYSTEM_DB, USERS_COLLECTION).is_err());
    }

    // Roles (ADR-073)
    // -----------------------------------------------------------------------

    #[test]
    fn role_grants_and_direct_grants_are_a_union() {
        let (engine, store, _dir) = setup();
        store
            .roles()
            .create(&engine, "reader", vec![Grant::new("sales", "*", vec![Action::Read])])
            .unwrap();
        store
            .create(&engine, "ada", "hunter2", vec![Grant::new("hr", "*", vec![Action::Write])])
            .unwrap();
        store.set_roles(&engine, "ada", vec!["reader".into()]).unwrap();

        let principal = store.authenticate(&engine, "ada", "hunter2").unwrap();

        assert!(
            principal.can(Action::Read, "sales", Some("orders")),
            "the role's grant is missing"
        );
        assert!(principal.can(Action::Write, "hr", Some("people")), "the direct grant was lost");
    }

    #[test]
    fn a_user_holding_no_roles_is_unchanged() {
        // The whole reason this needed no migration: `roles` defaults to empty
        // and an empty role list contributes nothing.
        let (engine, store, _dir) = setup();
        store
            .create(&engine, "ada", "hunter2", vec![Grant::new("hr", "*", vec![Action::Write])])
            .unwrap();

        let principal = store.authenticate(&engine, "ada", "hunter2").unwrap();

        assert_eq!(principal.grants, vec![Grant::new("hr", "*", vec![Action::Write])]);
    }

    #[test]
    fn narrowing_a_role_invalidates_the_tokens_of_everyone_holding_it() {
        // The one that matters. Grants are embedded in a token at login, so
        // without the bump a narrowed role would keep working until the token
        // expired — which is exactly the promise ADR-052 makes for set_grants.
        let (engine, store, _dir) = setup();
        store
            .roles()
            .create(
                &engine,
                "wide",
                vec![Grant::new("sales", "*", vec![Action::Read, Action::Write])],
            )
            .unwrap();
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();
        store.create(&engine, "grace", "hunter2", Vec::new()).unwrap();
        store.set_roles(&engine, "ada", vec!["wide".into()]).unwrap();

        let issued_at = store.authenticate(&engine, "ada", "hunter2").unwrap().token_version;
        let bystander = store.authenticate(&engine, "grace", "hunter2").unwrap().token_version;

        store
            .roles()
            .set_grants(&engine, "wide", vec![Grant::new("sales", "*", vec![Action::Read])])
            .unwrap();
        let invalidated = store.invalidate_holders_of_role(&engine, "wide").unwrap();

        assert_eq!(invalidated, 1, "only the holder should be invalidated");
        let after = store.authenticate(&engine, "ada", "hunter2").unwrap();
        assert_ne!(after.token_version, issued_at, "the holder's outstanding tokens still verify");
        assert!(!after.can(Action::Write, "sales", Some("orders")), "the narrowing did not apply");
        assert_eq!(
            store.authenticate(&engine, "grace", "hunter2").unwrap().token_version,
            bystander,
            "a user who does not hold the role was logged out for nothing"
        );
    }

    #[test]
    fn a_deleted_role_grants_nothing_and_is_not_an_error() {
        // A dangling name is the safe direction to fail in, and it is why a
        // delete does not have to rewrite every user record that names it.
        let (engine, store, _dir) = setup();
        store.roles().create(&engine, "temp", vec![Grant::superuser()]).unwrap();
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();
        store.set_roles(&engine, "ada", vec!["temp".into()]).unwrap();

        assert!(store.roles().delete(&engine, "temp").unwrap());

        let principal = store.authenticate(&engine, "ada", "hunter2").unwrap();
        assert!(principal.grants.is_empty(), "a deleted role still granted something");
    }

    #[test]
    fn assigning_a_role_invalidates_the_users_own_tokens() {
        // Widening needs the bump as much as narrowing does, so that the new
        // grants are actually reachable without waiting for expiry.
        let (engine, store, _dir) = setup();
        store
            .roles()
            .create(&engine, "reader", vec![Grant::new("sales", "*", vec![Action::Read])])
            .unwrap();
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();

        let before = store.authenticate(&engine, "ada", "hunter2").unwrap().token_version;
        store.set_roles(&engine, "ada", vec!["reader".into()]).unwrap();
        let after = store.authenticate(&engine, "ada", "hunter2").unwrap().token_version;

        assert_ne!(before, after);
    }
}
