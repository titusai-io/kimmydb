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

/// A role record as the engine stores it.
pub(crate) fn encode(role: &Role) -> Result<bson::Document> {
    bson::serialize_to_document(&StoredRole::from(role))
        .map_err(|e| AuthError::Hashing(format!("encoding role: {e}")))
}

/// A stored role record, read back.
pub(crate) fn decode(name: &str, doc: bson::Document) -> Result<Role> {
    bson::deserialize_from_document::<StoredRole>(doc)
        .map(Into::into)
        .map_err(|e| AuthError::Hashing(format!("decoding role {name:?}: {e}")))
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
        let Some(doc) = engine.get(&self.collection, &id).map_err(AuthError::Storage)? else {
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
            .map_err(AuthError::Storage)?;
        Ok(names)
    }

    pub fn create(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<Role> {
        if self.get(engine, name)?.is_some() {
            return Err(AuthError::RoleExists(name.to_string()));
        }
        let role = Role { name: name.to_string(), grants };
        let doc = bson::serialize_to_document(&StoredRole::from(&role))
            .map_err(|e| AuthError::Hashing(format!("encoding role: {e}")))?;
        engine.insert(&self.collection, doc).map_err(AuthError::Storage)?;
        Ok(role)
    }

    /// The collection role records live in.
    pub(crate) fn collection(&self) -> &CollectionMeta {
        &self.collection
    }

    /// Replace a role's grants, **without** invalidating its holders' tokens.
    ///
    /// For tests. The edit an administrator makes is
    /// [`crate::users::UserStore::set_role_grants`], which changes the role
    /// and every holder's token version in one transaction (ADR-192).
    #[cfg(test)]
    pub(crate) fn set_grants(&self, engine: &Engine, name: &str, grants: Vec<Grant>) -> Result<()> {
        let mut role =
            self.get(engine, name)?.ok_or_else(|| AuthError::RoleNotFound(name.into()))?;
        role.grants = grants;
        self.put(engine, &role)
    }

    #[cfg(test)]
    fn put(&self, engine: &Engine, role: &Role) -> Result<()> {
        let doc = bson::serialize_to_document(&StoredRole::from(role))
            .map_err(|e| AuthError::Hashing(format!("encoding role: {e}")))?;
        let id = DocId::String(role.name.clone());
        engine.replace(&self.collection, &id, doc, true).map_err(AuthError::Storage)?;
        Ok(())
    }

    /// Delete a role, **without** invalidating its holders' tokens.
    ///
    /// For tests; the administrator's delete is
    /// [`crate::users::UserStore::delete_role`].
    #[cfg(test)]
    pub(crate) fn delete(&self, engine: &Engine, name: &str) -> Result<bool> {
        let id = DocId::String(name.to_string());
        engine.delete(&self.collection, &id).map_err(AuthError::Storage)
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
