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
    #[serde(default)]
    pub disabled: bool,
}

impl User {
    fn to_principal(&self) -> Principal {
        Principal::new(self.name.clone(), self.grants.clone())
    }
}

/// Reads and writes users against the storage engine.
pub struct UserStore {
    collection: CollectionMeta,
}

impl UserStore {
    /// Open the store, creating the system collection if needed.
    pub fn open(engine: &Engine) -> Result<Self> {
        let collection = engine
            .create_system_collection(SYSTEM_DB, USERS_COLLECTION)
            .map_err(|e| AuthError::Hashing(format!("opening the user store: {e}")))?;
        Ok(Self { collection })
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
            disabled: false,
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

    pub fn delete(&self, engine: &Engine, name: &str) -> Result<bool> {
        let id = DocId::String(name.to_string());
        engine.delete(&self.collection, &id).map_err(storage_error)
    }

    pub fn set_password(&self, engine: &Engine, name: &str, password: &str) -> Result<()> {
        let mut user =
            self.get(engine, name)?.ok_or_else(|| AuthError::UserNotFound(name.into()))?;
        user.password_hash = password::hash(password)?;
        self.put(engine, &user)
    }

    pub fn set_grants(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<()> {
        let mut user =
            self.get(engine, name)?.ok_or_else(|| AuthError::UserNotFound(name.into()))?;
        user.grants = grants;
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

        Ok(user.to_principal())
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
}
