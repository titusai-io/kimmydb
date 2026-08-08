//! On-disk schema migrations.
//!
//! The record encoding ([`crate::codec::FORMAT_VERSION`]) and the database
//! *layout* are separate concerns and version separately. A record whose bytes
//! still decode does not need rewriting because the meaning of a key changed
//! around it — which is exactly the situation here.
//!
//! # Schema 2 — derived collection ids
//!
//! Collection ids used to come from a node-local counter. They are now derived
//! from `(database, name)`, so every node computes the same id for the same
//! collection ([`kimmy_core::CollectionId::derive`]).
//!
//! That renumbers every collection, and the id is embedded in three places:
//! document keys, index-entry keys, and the `collection` field of every oplog
//! entry. All three are rewritten here.
//!
//! Refusing to open instead would have been easier and is what the version
//! check does for a *newer* schema — but a database that can be migrated
//! should be, and a user with data has no other route forward.

use std::collections::HashMap;

use kimmy_core::CollectionId;
use redb::{Database, ReadableDatabase, ReadableTable};
use tracing::info;

use crate::codec;
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::tables;

/// The database layout this build writes and understands.
pub const SCHEMA_VERSION: u8 = 2;

/// Bring a database up to [`SCHEMA_VERSION`], or refuse if it cannot be.
pub(crate) fn run(db: &Database) -> Result<()> {
    let found = stored_version(db)?;

    match found {
        // A fresh database: nothing to migrate, just stamp it.
        None => write_version(db, SCHEMA_VERSION),
        Some(SCHEMA_VERSION) => Ok(()),
        Some(1) => {
            info!("migrating storage schema 1 -> 2 (derived collection ids)");
            derive_collection_ids(db)?;
            write_version(db, SCHEMA_VERSION)
        }
        // A newer schema means a newer build wrote this directory. Refusing is
        // the right failure: guessing at a layout we do not know would corrupt
        // it, and the version check exists precisely to avoid that.
        Some(other) => {
            Err(StorageError::UnsupportedFormat { found: other, expected: SCHEMA_VERSION })
        }
    }
}

fn stored_version(db: &Database) -> Result<Option<u8>> {
    let txn = db.begin_read()?;
    let meta = txn.open_table(tables::META)?;
    Ok(meta.get(tables::META_FORMAT_VERSION)?.and_then(|v| v.value().first().copied()))
}

fn write_version(db: &Database, version: u8) -> Result<()> {
    let txn = db.begin_write()?;
    {
        let mut meta = txn.open_table(tables::META)?;
        meta.insert(tables::META_FORMAT_VERSION, [version].as_slice())?;
    }
    txn.commit()?;
    Ok(())
}

/// Renumber every collection from its counter id to its derived id.
fn derive_collection_ids(db: &Database) -> Result<()> {
    let mut remap: HashMap<u64, u64> = HashMap::new();
    let mut updated: Vec<((String, String), CollectionMeta)> = Vec::new();

    {
        let txn = db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        for row in collections.iter()? {
            let (key, value) = row?;
            let (db_name, coll_name) = key.value();
            let mut meta: CollectionMeta = serde_json::from_slice(value.value())?;

            let new_id = CollectionId::derive(db_name, coll_name);
            if new_id != meta.id {
                remap.insert(meta.id.0, new_id.0);
            }
            meta.id = new_id;
            updated.push(((db_name.to_string(), coll_name.to_string()), meta));
        }
    }

    // Two collections whose derived ids collide would be merged by the
    // renumbering. Vanishingly unlikely, and checked anyway, because the
    // alternative is discovering it as interleaved data.
    let mut seen = HashMap::new();
    for ((db_name, coll_name), meta) in &updated {
        if let Some(previous) = seen.insert(meta.id.0, format!("{db_name}.{coll_name}")) {
            return Err(StorageError::Corrupt(format!(
                "cannot migrate: {db_name}.{coll_name} and {previous} derive the same collection \
                 id; rename one of them with the previous build first"
            )));
        }
    }

    if remap.is_empty() {
        return Ok(());
    }

    // One collection at a time, so a large database does not have to hold its
    // whole document set in memory to be migrated.
    for (old, new) in &remap {
        move_documents(db, *old, *new)?;
        move_index_entries(db, *old, *new)?;
    }
    rewrite_oplog(db, &remap)?;

    let txn = db.begin_write()?;
    {
        let mut collections = txn.open_table(tables::COLLECTIONS)?;
        for ((db_name, coll_name), meta) in &updated {
            collections.insert(
                (db_name.as_str(), coll_name.as_str()),
                serde_json::to_vec(meta)?.as_slice(),
            )?;
        }
    }
    txn.commit()?;

    info!(collections = remap.len(), "renumbered collections to derived ids");
    Ok(())
}

fn move_documents(db: &Database, old: u64, new: u64) -> Result<()> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        docs.range(crate::engine::doc_range(CollectionId(old)))?
            .map(|row| {
                let (key, value) = row?;
                Ok((key.value().1.to_vec(), value.value().to_vec()))
            })
            .collect::<Result<_>>()?
    };

    let txn = db.begin_write()?;
    {
        let mut docs = txn.open_table(tables::DOCS)?;
        for (key, value) in &rows {
            docs.insert((new, key.as_slice()), value.as_slice())?;
        }
        docs.retain_in(crate::engine::doc_range(CollectionId(old)), |_, _| false)?;
    }
    txn.commit()?;
    Ok(())
}

fn move_index_entries(db: &Database, old: u64, new: u64) -> Result<()> {
    #[allow(clippy::type_complexity)]
    let rows: Vec<(u32, Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let entries = txn.open_table(tables::INDEX_ENTRIES)?;
        entries
            .range(crate::engine::index_range(CollectionId(old)))?
            .map(|row| {
                let (key, _) = row?;
                let (_, index_id, value, doc_key) = key.value();
                Ok((index_id, value.to_vec(), doc_key.to_vec()))
            })
            .collect::<Result<_>>()?
    };

    let txn = db.begin_write()?;
    {
        let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
        for (index_id, value, doc_key) in &rows {
            entries.insert((new, *index_id, value.as_slice(), doc_key.as_slice()), ())?;
        }
        entries.retain_in(crate::engine::index_range(CollectionId(old)), |_, _| false)?;
    }
    txn.commit()?;
    Ok(())
}

/// Rewrite the `collection` field of every affected oplog entry.
///
/// Keys are stamps and do not change, so this is a value rewrite in place —
/// which also means the arrival index, which maps sequence to stamp, needs no
/// migration at all.
fn rewrite_oplog(db: &Database, remap: &HashMap<u64, u64>) -> Result<()> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let mut out = Vec::new();
        for row in oplog.iter()? {
            let (key, value) = row?;
            let mut entry = codec::decode_oplog_entry(value.value())?;
            let Some(new) = remap.get(&entry.collection.0) else {
                continue;
            };
            entry.collection = CollectionId(*new);
            out.push((key.value().to_vec(), codec::encode_oplog_entry(&entry)));
        }
        out
    };

    if rows.is_empty() {
        return Ok(());
    }

    let txn = db.begin_write()?;
    {
        let mut oplog = txn.open_table(tables::OPLOG)?;
        for (key, value) in &rows {
            oplog.insert(key.as_slice(), value.as_slice())?;
        }
    }
    txn.commit()?;

    info!(entries = rows.len(), "repointed oplog entries to derived collection ids");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use bson::doc;
    use kimmy_core::DocId;

    /// Rewind a database to schema 1: counter ids, and the version to match.
    ///
    /// Builds the old layout from the new one rather than checking in a binary
    /// fixture, so the test keeps working as the rest of the format evolves.
    fn rewind_to_schema_1(path: &std::path::Path, assignments: &[(&str, &str, u64)]) {
        let db = Database::create(path).unwrap();

        let mut remap = HashMap::new();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
                for (db_name, coll_name, old_id) in assignments {
                    let raw = collections.get((*db_name, *coll_name)).unwrap().unwrap();
                    let mut meta: CollectionMeta = serde_json::from_slice(raw.value()).unwrap();
                    drop(raw);
                    remap.insert(meta.id.0, *old_id);
                    meta.id = CollectionId(*old_id);
                    collections
                        .insert(
                            (*db_name, *coll_name),
                            serde_json::to_vec(&meta).unwrap().as_slice(),
                        )
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }

        for (new, old) in &remap {
            move_documents(&db, *new, *old).unwrap();
            move_index_entries(&db, *new, *old).unwrap();
        }
        rewrite_oplog(&db, &remap).unwrap();
        write_version(&db, 1).unwrap();
    }

    #[test]
    fn a_schema_1_database_is_migrated_rather_than_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            engine.create_index("shop", "orders", vec![field("item")], false, None).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            for i in 0..5i64 {
                engine.insert(&orders, doc! { "_id": i, "item": format!("w{i}") }).unwrap();
            }
        }

        rewind_to_schema_1(&path, &[("shop", "orders", 7)]);

        // Reopening must migrate, not refuse.
        let engine = Engine::open(&path).unwrap();
        let orders = engine.get_collection("shop", "orders").unwrap();

        assert_eq!(orders.id, CollectionId::derive("shop", "orders"), "id must be renumbered");
        assert_eq!(engine.count(&orders).unwrap(), 5, "documents must move with the id");
        assert!(engine.get(&orders, &DocId::Int64(3)).unwrap().is_some());
    }

    #[test]
    fn migrated_index_entries_still_answer_queries() {
        // Index keys embed the collection id, so an unmigrated index entry is
        // an index that silently finds nothing.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            engine.create_index("shop", "orders", vec![field("item")], false, None).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1, "item": "widget" }).unwrap();
        }

        rewind_to_schema_1(&path, &[("shop", "orders", 9)]);
        let engine = Engine::open(&path).unwrap();
        let orders = engine.get_collection("shop", "orders").unwrap();

        let index = &orders.indexes[0];
        let key = kimmy_core::keyenc::encode(&bson::Bson::String("widget".into())).unwrap();
        let found = engine.index_candidates(&orders, index.id, &key, &key).unwrap();
        assert_eq!(found.len(), 1, "the index must follow the collection to its new id");
    }

    #[test]
    fn migrated_oplog_entries_point_at_the_new_id() {
        // Otherwise a change stream would attribute history to a collection
        // that no longer has that id — and the embedding worker would skip it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1 }).unwrap();
        }

        rewind_to_schema_1(&path, &[("shop", "orders", 42)]);
        let engine = Engine::open(&path).unwrap();

        let expected = CollectionId::derive("shop", "orders");
        let entries = engine.read_arrival_from(0, 100).unwrap();
        assert!(
            entries.iter().all(|e| e.collection != CollectionId(42)),
            "no entry may still name the old id"
        );
        assert!(
            entries.iter().any(|e| e.collection == expected),
            "entries must be repointed at the derived id"
        );
    }

    #[test]
    fn migration_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1 }).unwrap();
        }
        rewind_to_schema_1(&path, &[("shop", "orders", 5)]);

        for _ in 0..3 {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            assert_eq!(orders.id, CollectionId::derive("shop", "orders"));
            assert_eq!(engine.count(&orders).unwrap(), 1);
        }
    }

    #[test]
    fn a_newer_schema_is_refused_rather_than_guessed_at() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let _ = Engine::open(&path).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            write_version(&db, SCHEMA_VERSION + 1).unwrap();
        }

        let err = Engine::open(&path).err().expect("a future layout must not be opened");
        assert!(
            matches!(err, StorageError::UnsupportedFormat { .. }),
            "expected an unsupported-format refusal, got {err:?}"
        );
    }

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }
}
