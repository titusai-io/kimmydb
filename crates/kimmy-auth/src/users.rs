//! The persistent user store.
//!
//! Users live in an ordinary collection in a reserved system database, so they
//! get the same durability, oplog, and (eventually) replication as any other
//! data. The `__` name prefix is rejected for user-created objects precisely so
//! that nothing can collide with these.

use kimmy_core::DocId;
use kimmy_storage::{CollectionMeta, Engine, WriteScope, WriterHolder};
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

/// A user record as the engine stores it.
fn encode(user: &User) -> Result<bson::Document> {
    bson::serialize_to_document(user).map_err(|e| AuthError::Hashing(format!("encoding user: {e}")))
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
        self.edit(engine, name, |user| user.roles = roles)
    }

    /// Replace a role's grants and invalidate every token held by a user
    /// carrying it, returning those users.
    ///
    /// **The invalidation is what keeps a role edit honest, and it is the
    /// easiest thing in the feature to leave out.** A local user's grants are
    /// resolved at login and embedded in its token, so narrowing a role changes
    /// nothing for anyone already holding one until it expires — silently
    /// contradicting the promise [`Self::set_grants`] has made since ADR-052.
    ///
    /// The names, not a count: bumping the stored version is only half of a
    /// revocation, because the session check reads a cache in front of it
    /// (ADR-052). The caller has to evict each holder from that cache, and it
    /// cannot do that from a number.
    ///
    /// **One transaction** for the role and every holder (ADR-192). The role
    /// was one commit and each holder another, so a failure part way answered
    /// an error over a role already changed, and left the holders not yet
    /// reached holding tokens with the old grants. And each holder was read
    /// outside the writer and written back whole, so an edit that landed in
    /// between — an account disabled a moment before — was reverted.
    ///
    /// Federated principals need nothing here and get nothing: they have no
    /// user record and no token version (ADR-065), and their grants are
    /// resolved from the mapping on every request, so a role edit already
    /// applies to them immediately.
    pub fn set_role_grants(
        &self,
        engine: &Engine,
        role: &str,
        grants: Vec<Grant>,
    ) -> Result<Vec<String>> {
        let roles = self.roles.collection().clone();
        self.edit_role(engine, role, |scope| {
            let id = DocId::String(role.to_string());
            let Some(doc) = scope.get(&roles, &id)? else {
                return Ok(Err(AuthError::RoleNotFound(role.into())));
            };
            let mut stored = match crate::roles::decode(role, doc) {
                Ok(stored) => stored,
                Err(e) => return Ok(Err(e)),
            };
            stored.grants = grants;
            let doc = match crate::roles::encode(&stored) {
                Ok(doc) => doc,
                Err(e) => return Ok(Err(e)),
            };
            scope.replace(&roles, &id, doc, true)?;
            Ok(Ok(()))
        })
        .map(|((), holders)| holders)
    }

    /// Delete a role and invalidate every token held by a user carrying it,
    /// in one transaction (ADR-192); see [`Self::set_role_grants`]. Returns
    /// whether the role existed, and the users invalidated.
    ///
    /// Holders keep the role *name* on their record, where it resolves to
    /// nothing. That is deliberate: the alternative is editing every user
    /// record's roles on a delete, and a dangling name that grants nothing is
    /// the safe direction to fail in. Their tokens are bumped whether or not
    /// the role still existed, so a delete sent again reaches every holder.
    pub fn delete_role(&self, engine: &Engine, role: &str) -> Result<(bool, Vec<String>)> {
        let roles = self.roles.collection().clone();
        self.edit_role(engine, role, |scope| {
            Ok(Ok(scope.delete(&roles, &DocId::String(role.to_string()))?))
        })
    }

    /// Change a role through `change`, then bump the token version of every
    /// user holding it, all in one scope: every record is read under the
    /// writer, in the transaction that writes it.
    ///
    /// `change` answers the storage error that aborts the transaction outside,
    /// and a refusal of its own inside; a refusal aborts it too, having
    /// written nothing, because nothing is written after it.
    fn edit_role<T>(
        &self,
        engine: &Engine,
        role: &str,
        change: impl FnOnce(&mut WriteScope<'_>) -> kimmy_storage::Result<Result<T>>,
    ) -> Result<(T, Vec<String>)> {
        let users = self.collection.clone();
        let edited = engine.write_batch(WriterHolder::Write, |scope| {
            let mut holders = Vec::new();
            let mut undecodable = None;
            scope.for_each_doc(&users, |id, doc| {
                match bson::deserialize_from_document::<User>(doc.clone()) {
                    Ok(user) if user.roles.iter().any(|held| held == role) => holders.push(user),
                    Ok(_) => {}
                    Err(e) => {
                        undecodable = Some(AuthError::Hashing(format!("decoding user {id}: {e}")));
                        return Ok(false);
                    }
                }
                Ok(true)
            })?;
            if let Some(e) = undecodable {
                return Ok(Err(e));
            }
            // Encoded before anything is written, so a refusal here leaves the
            // scope unwritten and the transaction aborts whole.
            let mut bumped = Vec::with_capacity(holders.len());
            for mut user in holders {
                user.token_version = user.token_version.wrapping_add(1);
                match encode(&user) {
                    Ok(doc) => bumped.push((user.name, doc)),
                    Err(e) => return Ok(Err(e)),
                }
            }
            let value = match change(scope)? {
                Ok(value) => value,
                Err(e) => return Ok(Err(e)),
            };
            let mut names = Vec::with_capacity(bumped.len());
            for (name, doc) in bumped {
                scope.replace(&users, &DocId::String(name.clone()), doc, true)?;
                names.push(name);
            }
            Ok(Ok((value, names)))
        });
        edited.map_err(AuthError::Storage)?
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
        engine.count(&self.collection).map_err(AuthError::Storage)
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
        engine.insert(&self.collection, doc).map_err(AuthError::Storage)?;
        Ok(user)
    }

    pub fn get(&self, engine: &Engine, name: &str) -> Result<Option<User>> {
        let id = DocId::String(name.to_string());
        let Some(doc) = engine.get(&self.collection, &id).map_err(AuthError::Storage)? else {
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
            .map_err(AuthError::Storage)?;
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
        engine.delete(&self.collection, &id).map_err(AuthError::Storage)
    }

    /// Set a new password, ending every session the old one opened.
    ///
    /// The bump is the conventional behaviour and the one someone expects
    /// after a suspected compromise: changing the password logs out everyone
    /// holding a token for this account, including whoever took it.
    pub fn set_password(&self, engine: &Engine, name: &str, password: &str) -> Result<()> {
        // Hashed before the writer is taken: it is deliberately slow, and
        // every other write on the node would wait behind it.
        let hash = password::hash(password)?;
        self.edit(engine, name, |user| user.password_hash = hash)
    }

    /// Replace a user's grants, taking effect immediately.
    ///
    /// Grants are embedded in the token, so without the bump an edit — a
    /// *narrowing* one especially — would do nothing until the token expired.
    /// The cost is that there is no refresh flow, so this logs the user out.
    pub fn set_grants(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<()> {
        self.edit(engine, name, |user| user.grants = grants)
    }

    /// Disable or re-enable an account, taking effect immediately.
    ///
    /// A disabled account is refused at authentication while its record stays
    /// exactly where it is — the reversible form of deletion, and the right
    /// shape for a leaver whose identifier an external issuer may still
    /// assert. As with every other edit here, the bump to `token_version`
    /// logs the account out of every session it holds; **re-enabling does not
    /// restore them**, which is the point.
    ///
    /// Disabling the last enabled account is refused ([`AuthError::LastEnabledUser`]),
    /// decided under the writer in the transaction that would disable it: two
    /// administrators disabling each other at once used to both pass a check
    /// made outside it, and leave no enabled account (ADR-192).
    pub fn set_disabled(&self, engine: &Engine, name: &str, disabled: bool) -> Result<()> {
        let users = self.collection.clone();
        self.edit_checked(
            engine,
            name,
            |scope, user| {
                if !disabled || user.disabled {
                    return Ok(Ok(()));
                }
                let mut enabled_others = 0usize;
                scope.for_each_doc(&users, |id, doc| {
                    let other_enabled = doc.get_bool("disabled").map(|d| !d).unwrap_or(true);
                    if other_enabled && id != DocId::String(user.name.clone()) {
                        enabled_others += 1;
                    }
                    Ok(enabled_others == 0)
                })?;
                Ok(if enabled_others == 0 { Err(AuthError::LastEnabledUser) } else { Ok(()) })
            },
            |user| user.disabled = disabled,
        )
    }

    /// Delete a user unless it is the last one, decided under the writer in
    /// the transaction that deletes it ([`AuthError::LastUser`], ADR-192).
    /// Returns whether it existed.
    pub fn delete_unless_last(&self, engine: &Engine, name: &str) -> Result<bool> {
        let users = self.collection.clone();
        let deleted = engine.write_batch(WriterHolder::Write, |scope| {
            let mut count = 0usize;
            scope.for_each_doc(&users, |_, _| {
                count += 1;
                Ok(count < 2)
            })?;
            if count <= 1 {
                return Ok(Err(AuthError::LastUser));
            }
            Ok(Ok(scope.delete(&users, &DocId::String(name.to_string()))?))
        });
        deleted.map_err(AuthError::Storage)?
    }

    /// Change one user record and bump its token version, reading it under
    /// the writer in the transaction that writes it back (ADR-192).
    ///
    /// Every setter used to read the record, change it, and replace it whole
    /// in a transaction of its own, so two edits racing on one user each
    /// wrote back what the other had not seen: a grant change landing just
    /// after an account was disabled re-enabled it.
    fn edit(&self, engine: &Engine, name: &str, change: impl FnOnce(&mut User)) -> Result<()> {
        self.edit_checked(engine, name, |_, _| Ok(Ok(())), change)
    }

    /// [`Self::edit`], refused by `check` — which reads, in the same
    /// transaction, whatever it needs to decide — before anything is written.
    fn edit_checked(
        &self,
        engine: &Engine,
        name: &str,
        check: impl FnOnce(&WriteScope<'_>, &User) -> kimmy_storage::Result<Result<()>>,
        change: impl FnOnce(&mut User),
    ) -> Result<()> {
        let users = self.collection.clone();
        let edited = engine.write_batch(WriterHolder::Write, |scope| {
            let id = DocId::String(name.to_string());
            let Some(doc) = scope.get(&users, &id)? else {
                return Ok(Err(AuthError::UserNotFound(name.into())));
            };
            let mut user: User = match bson::deserialize_from_document(doc) {
                Ok(user) => user,
                Err(e) => {
                    return Ok(Err(AuthError::Hashing(format!("decoding user {name:?}: {e}"))));
                }
            };
            if let Err(e) = check(scope, &user)? {
                return Ok(Err(e));
            }
            change(&mut user);
            user.token_version = user.token_version.wrapping_add(1);
            let doc = match encode(&user) {
                Ok(doc) => doc,
                Err(e) => return Ok(Err(e)),
            };
            scope.replace(&users, &id, doc, true)?;
            Ok(Ok(()))
        });
        edited.map_err(AuthError::Storage)?
    }

    fn put(&self, engine: &Engine, user: &User) -> Result<()> {
        let doc = encode(user)?;
        let id = DocId::String(user.name.clone());
        engine.replace(&self.collection, &id, doc, true).map_err(AuthError::Storage)?;
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

        let before = engine.commits();
        let invalidated = store
            .set_role_grants(&engine, "wide", vec![Grant::new("sales", "*", vec![Action::Read])])
            .unwrap();
        assert_eq!(engine.commits() - before, 1, "the role and its holders in one commit");

        assert_eq!(invalidated, vec!["ada".to_string()], "only the holder should be invalidated");
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

    #[test]
    fn deleting_a_role_invalidates_its_holders_in_one_commit_and_a_resend_reaches_them_again() {
        let (engine, store, _dir) = setup();
        store.roles().create(&engine, "temp", vec![Grant::superuser()]).unwrap();
        for name in ["ada", "grace", "linus"] {
            store.create(&engine, name, "hunter2", Vec::new()).unwrap();
        }
        store.set_roles(&engine, "ada", vec!["temp".into()]).unwrap();
        store.set_roles(&engine, "grace", vec!["temp".into()]).unwrap();

        let before = engine.commits();
        let (deleted, holders) = store.delete_role(&engine, "temp").unwrap();
        assert!(deleted);
        assert_eq!(holders, vec!["ada".to_string(), "grace".to_string()]);
        assert_eq!(engine.commits() - before, 1, "the role and both holders together");

        // The role is gone, and the holders still name it: a delete sent
        // again bumps them again rather than stopping at "no such role".
        let version = store.get(&engine, "ada").unwrap().unwrap().token_version;
        let (deleted, holders) = store.delete_role(&engine, "temp").unwrap();
        assert!(!deleted);
        assert_eq!(holders.len(), 2);
        assert_eq!(store.get(&engine, "ada").unwrap().unwrap().token_version, version + 1);
    }

    #[test]
    fn a_role_edit_of_a_role_that_does_not_exist_writes_nothing() {
        let (engine, store, _dir) = setup();
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();
        store.set_roles(&engine, "ada", vec!["ghost".into()]).unwrap();
        let version = store.get(&engine, "ada").unwrap().unwrap().token_version;
        let before = engine.commits();
        assert!(matches!(
            store.set_role_grants(&engine, "ghost", Vec::new()),
            Err(AuthError::RoleNotFound(_))
        ));
        assert_eq!(engine.commits(), before, "nothing committed");
        assert_eq!(store.get(&engine, "ada").unwrap().unwrap().token_version, version);
    }

    /// Queue `edits` behind a writer this test holds, each started a little
    /// after the one before so that they queue in that order, then let go and
    /// wait for all of them. Whatever an edit reads before it takes the
    /// writer, it reads before any of the others has written.
    fn queued_behind_the_writer(
        engine: &std::sync::Arc<Engine>,
        edits: Vec<Box<dyn FnOnce() + Send>>,
    ) {
        let hold = engine.hold_writer(WriterHolder::Write);
        let handles: Vec<_> = edits
            .into_iter()
            .map(|edit| {
                let handle = std::thread::spawn(edit);
                std::thread::sleep(std::time::Duration::from_millis(100));
                handle
            })
            .collect();
        drop(hold);
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn two_edits_racing_on_one_user_both_land() {
        // Each setter read the record, changed it, and wrote it back whole in
        // a transaction of its own, so the second to commit undid the first.
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let store = std::sync::Arc::new(UserStore::open(&engine).unwrap());
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();
        // Someone else enabled, so disabling ada is allowed.
        store.create(&engine, "grace", "hunter2", Vec::new()).unwrap();
        let base = store.get(&engine, "ada").unwrap().unwrap().token_version;

        let grants = vec![Grant::new("sales", "*", vec![Action::Read])];
        let (e1, s1) = (std::sync::Arc::clone(&engine), std::sync::Arc::clone(&store));
        let (e2, s2, g2) =
            (std::sync::Arc::clone(&engine), std::sync::Arc::clone(&store), grants.clone());
        queued_behind_the_writer(
            &engine,
            vec![
                Box::new(move || s1.set_disabled(&e1, "ada", true).unwrap()),
                Box::new(move || s2.set_grants(&e2, "ada", g2).unwrap()),
            ],
        );

        let ada = store.get(&engine, "ada").unwrap().unwrap();
        assert!(ada.disabled, "the grant edit re-enabled the account");
        assert_eq!(ada.grants, grants, "the disable undid the grant edit");
        assert_eq!(ada.token_version, base + 2, "a bump was lost");
    }

    #[test]
    fn a_role_edit_racing_a_disable_leaves_the_account_disabled() {
        // The HIGH finding: the role edit read each holder outside the writer
        // and wrote it back whole, so an account disabled a moment before was
        // enabled again. The disable queues first and commits first; the
        // role edit, queued behind it, must see it.
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let store = std::sync::Arc::new(UserStore::open(&engine).unwrap());
        store.roles().create(&engine, "wide", vec![Grant::superuser()]).unwrap();
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();
        store.create(&engine, "grace", "hunter2", Vec::new()).unwrap();
        store.set_roles(&engine, "ada", vec!["wide".into()]).unwrap();
        let base = store.get(&engine, "ada").unwrap().unwrap().token_version;

        let (e1, s1) = (std::sync::Arc::clone(&engine), std::sync::Arc::clone(&store));
        let (e2, s2) = (std::sync::Arc::clone(&engine), std::sync::Arc::clone(&store));
        queued_behind_the_writer(
            &engine,
            vec![
                Box::new(move || s1.set_disabled(&e1, "ada", true).unwrap()),
                Box::new(move || {
                    s2.set_role_grants(&e2, "wide", Vec::new()).unwrap();
                }),
            ],
        );

        let ada = store.get(&engine, "ada").unwrap().unwrap();
        assert!(ada.disabled, "the role edit re-enabled an account an admin had just disabled");
        assert_eq!(ada.token_version, base + 2, "a bump was lost");
    }

    /// Two edits, each queued behind a held writer, that must not both land.
    /// Returns what each answered.
    fn racing(
        engine: &std::sync::Arc<Engine>,
        store: &std::sync::Arc<UserStore>,
        first: fn(&UserStore, &Engine) -> Result<()>,
        second: fn(&UserStore, &Engine) -> Result<()>,
    ) -> (Result<()>, Result<()>) {
        let answers = std::sync::Arc::new(std::sync::Mutex::new((None, None)));
        let (e1, s1, a1) =
            (std::sync::Arc::clone(engine), std::sync::Arc::clone(store), answers.clone());
        let (e2, s2, a2) =
            (std::sync::Arc::clone(engine), std::sync::Arc::clone(store), answers.clone());
        queued_behind_the_writer(
            engine,
            vec![
                Box::new(move || a1.lock().unwrap().0 = Some(first(&s1, &e1))),
                Box::new(move || a2.lock().unwrap().1 = Some(second(&s2, &e2))),
            ],
        );
        let (a, b) = std::mem::take(&mut *answers.lock().unwrap());
        (a.unwrap(), b.unwrap())
    }

    fn two_users() -> (std::sync::Arc<Engine>, std::sync::Arc<UserStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let store = std::sync::Arc::new(UserStore::open(&engine).unwrap());
        store.create(&engine, "ada", "hunter2", Vec::new()).unwrap();
        store.create(&engine, "grace", "hunter2", Vec::new()).unwrap();
        (engine, store, dir)
    }

    #[test]
    fn two_admins_disabling_each_other_at_once_leave_one_enabled() {
        // The last-enabled guard was checked outside the writer, so both
        // disables passed it and no account was left enabled.
        let (engine, store, _dir) = two_users();
        let (a, b) = racing(
            &engine,
            &store,
            |s, e| s.set_disabled(e, "ada", true),
            |s, e| s.set_disabled(e, "grace", true),
        );
        assert!(a.is_ok(), "{a:?}");
        assert!(matches!(b, Err(AuthError::LastEnabledUser)), "{b:?}");
        assert!(!store.get(&engine, "grace").unwrap().unwrap().disabled);
    }

    #[test]
    fn two_deletes_at_once_leave_one_user() {
        let (engine, store, _dir) = two_users();
        let (a, b) = racing(
            &engine,
            &store,
            |s, e| s.delete_unless_last(e, "ada").map(|_| ()),
            |s, e| s.delete_unless_last(e, "grace").map(|_| ()),
        );
        assert!(a.is_ok(), "{a:?}");
        assert!(matches!(b, Err(AuthError::LastUser)), "{b:?}");
        assert_eq!(store.count(&engine).unwrap(), 1);
    }

    /// How long a role edit holds the writer at 10,000 holders (ADR-192).
    /// Run by hand, in two processes, so the OS page cache can be dropped
    /// between them: `KIMMY_HOLD_BENCH=create` builds the store in
    /// `KIMMY_HOLD_BENCH_DIR`, and `KIMMY_HOLD_BENCH=edit` opens it and edits
    /// the role twice, printing the hold each time.
    #[test]
    #[ignore = "a measurement, run by hand"]
    fn a_role_edit_at_ten_thousand_holders_holds_the_writer_for() {
        let dir = std::path::PathBuf::from(std::env::var("KIMMY_HOLD_BENCH_DIR").unwrap());
        let path = dir.join("kimmy.redb");
        match std::env::var("KIMMY_HOLD_BENCH").unwrap().as_str() {
            "create" => {
                let engine = Engine::open(&path).unwrap();
                let store = UserStore::open(&engine).unwrap();
                store.roles().create(&engine, "wide", vec![Grant::superuser()]).unwrap();
                let hash = password::hash("hunter2").unwrap();
                let users = store.collection.clone();
                for batch in 0..10 {
                    let docs = (0..1_000)
                        .map(|i| {
                            encode(&User {
                                name: format!("user{:05}", batch * 1_000 + i),
                                password_hash: hash.clone(),
                                grants: Vec::new(),
                                roles: vec!["wide".into()],
                                disabled: false,
                                token_version: 0,
                            })
                            .unwrap()
                        })
                        .collect();
                    engine.insert_many(&users, docs).unwrap();
                }
            }
            "edit" => {
                // The engine's own cache is cold because the engine is new;
                // the OS page cache is whatever the caller left it.
                let engine = Engine::open(&path).unwrap();
                let store = UserStore::open(&engine).unwrap();
                for run in ["cold", "warm"] {
                    let started = std::time::Instant::now();
                    let holders = store.set_role_grants(&engine, "wide", Vec::new()).unwrap();
                    println!(
                        "{run}: {} holders, call {:?}, longest writer hold {:?}",
                        holders.len(),
                        started.elapsed(),
                        engine.writer_hold_max()
                    );
                }
            }
            other => panic!("KIMMY_HOLD_BENCH={other}: create or edit"),
        }
    }
}
