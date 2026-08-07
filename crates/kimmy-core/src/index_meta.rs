//! Index definitions.
//!
//! Lives in `kimmy-core` because both `kimmy-storage` (which maintains index
//! entries) and `kimmy-query` (which plans against them) need this shape, and
//! neither should depend on the other.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    pub id: u32,
    pub name: String,
    pub fields: Vec<IndexField>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexField {
    /// Dot path into the document, e.g. `"address.city"`.
    pub path: String,
    #[serde(default)]
    pub descending: bool,
}

impl IndexField {
    pub fn ascending(path: impl Into<String>) -> Self {
        Self { path: path.into(), descending: false }
    }

    pub fn descending(path: impl Into<String>) -> Self {
        Self { path: path.into(), descending: true }
    }
}

impl IndexMeta {
    /// Conventional Mongo-style name, e.g. `age_1_name_-1`.
    pub fn default_name(fields: &[IndexField]) -> String {
        fields
            .iter()
            .map(|f| format!("{}_{}", f.path, if f.descending { -1 } else { 1 }))
            .collect::<Vec<_>>()
            .join("_")
    }

    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.path.as_str())
    }
}
