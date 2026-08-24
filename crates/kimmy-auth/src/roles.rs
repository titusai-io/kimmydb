//! The persistent role store.
//!
//! Roles live in an ordinary collection in the reserved system database, next
//! to `__users` and for the same reasons: same durability, same oplog, same
//! replication. The `__` name prefix is rejected for user-created objects, so
//! nothing can collide with these.
//!
//! # Why this exists (ADR-073)
//!
//! Grants used to be reachable two different ways. A local user carried them
//! directly on its record — an ACL, copied onto every principal — while a
//! federated user got them from an IdP claim through
//! `[[auth.oidc.role_mappings]]`, which is RBAC. "analyst" therefore meant one
//! thing in a config file and a hand-assembled copy of that thing on each user
//! record, with nothing keeping the two in agreement.
//!
//! A role is one object naming one set of grants. Both paths can point at it.
//!
//! # What a role does not change
//!
//! The collection is still the ceiling. Roles change *who holds* a permission
//! and how it is administered; they do not change how finely it cuts. There is
//! still no document- or field-level security (ADR-076, `docs/security.md`).

use kimmy_core::DocId;
use kimmy_storage::{CollectionMeta, Engine};
use serde::{Deserialize, Serialize};

use crate::error::{AuthError, Result};
use crate::rbac::{Grant, Role};
use crate::users::SYSTEM_DB;

/// Collection holding role records.
pub const ROLES_COLLECTION: &str = "__roles";

/// A role as stored.
///
/// Separate from [`Role`] for one reason: the engine keys a document by `_id`,
/// and [`Role`] is also the shape the admin API speaks, where a field called
/// `_id` would be leaking storage into a public contract. The conversion is the
/// whole difference.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredRole {
    #[serde(rename = "_id")]
    name: String,
    #[serde(default)]
    grants: Vec<Grant>,
}

impl From<StoredRole> for Role {
    fn from(stored: StoredRole) -> Self {
        Role { name: stored.name, grants: stored.grants }
    }
}

impl From<&Role> for StoredRole {
    fn from(role: &Role) -> Self {
        StoredRole { name: role.name.clone(), grants: role.grants.clone() }
    }
}

/// Reads and writes roles against the storage engine.
pub struct RoleStore {
    collection: CollectionMeta,
}

impl RoleStore {
    /// Open the store, creating the system collection if needed.
    ///
    /// No migration and no schema bump: a system collection is created on
    /// demand, exactly as `__users` is, so an existing database grows one the
    /// first time a node with this build opens it.
    pub fn open(engine: &Engine) -> Result<Self> {
        let collection = engine
            .create_system_collection(SYSTEM_DB, ROLES_COLLECTION)
            .map_err(|e| AuthError::Hashing(format!("opening the role store: {e}")))?;
        Ok(Self { collection })
    }

    pub fn get(&self, engine: &Engine, name: &str) -> Result<Option<Role>> {
        let id = DocId::String(name.to_string());
        let Some(doc) = engine.get(&self.collection, &id).map_err(storage_error)? else {
            return Ok(None);
        };
        bson::deserialize_from_document::<StoredRole>(doc)
            .map(|stored| Some(stored.into()))
            .map_err(|e| AuthError::Hashing(format!("decoding role {name:?}: {e}")))
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

    pub fn create(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<Role> {
        if self.get(engine, name)?.is_some() {
            return Err(AuthError::RoleExists(name.to_string()));
        }
        let role = Role { name: name.to_string(), grants };
        let doc = bson::serialize_to_document(&StoredRole::from(&role))
            .map_err(|e| AuthError::Hashing(format!("encoding role: {e}")))?;
        engine.insert(&self.collection, doc).map_err(storage_error)?;
        Ok(role)
    }

    /// Replace a role's grants.
    ///
    /// **The caller must invalidate every holder's tokens afterwards** — see
    /// [`crate::users::UserStore::invalidate_holders_of_role`] and the note on
    /// [`Self::delete`]. This method deliberately does not do it itself: it
    /// holds no reference to the user store, and hiding a scan of every user
    /// record inside a setter is worse than making the caller say so.
    pub fn set_grants(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<()> {
        let mut role =
            self.get(engine, name)?.ok_or_else(|| AuthError::RoleNotFound(name.into()))?;
        role.grants = grants;
        self.put(engine, &role)
    }

    fn put(&self, engine: &Engine, role: &Role) -> Result<()> {
        let doc = bson::serialize_to_document(&StoredRole::from(role))
            .map_err(|e| AuthError::Hashing(format!("encoding role: {e}")))?;
        let id = DocId::String(role.name.clone());
        engine.replace(&self.collection, &id, doc, true).map_err(storage_error)?;
        Ok(())
    }

    /// Delete a role.
    ///
    /// Holders keep the role *name* on their record, where it resolves to
    /// nothing. That is deliberate: the alternative is editing every user
    /// record on a delete, and a dangling name that grants nothing is the safe
    /// direction to fail in. As with [`Self::set_grants`], the caller must
    /// invalidate holders' tokens.
    pub fn delete(&self, engine: &Engine, name: &str) -> Result<bool> {
        let id = DocId::String(name.to_string());
        engine.delete(&self.collection, &id).map_err(storage_error)
    }

    /// The grants named by `roles`, concatenated.
    ///
    /// A name that resolves to nothing contributes nothing. It is not an error:
    /// a role can be deleted while a user still names it, and a principal with
    /// fewer grants than expected is the safe reading of that.
    pub fn grants_for(&self, engine: &Engine, roles: &[String]) -> Result<Vec<Grant>> {
        let mut grants = Vec::new();
        for name in roles {
            if let Some(role) = self.get(engine, name)? {
                grants.extend(role.grants);
            }
        }
        Ok(grants)
    }
}

fn storage_error(e: kimmy_storage::StorageError) -> AuthError {
    AuthError::Hashing(format!("role store: {e}"))
}
