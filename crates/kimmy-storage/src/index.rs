//! Secondary index maintenance.
//!
//! Index entries are written in the **same transaction** as the document they
//! describe. Anything less and an index can disagree with the data it indexes,
//! which does not crash — it silently returns wrong query results.
//!
//! An index is only ever an *access path*: it answers "which documents might
//! match", and the caller must still apply the full filter to every candidate.
//! The one exception is a unique index, which additionally enforces a
//! constraint.

use bson::{Bson, Document};
use kimmy_core::{CollectionId, DocId, Error as CoreError, keyenc, path};
use redb::{ReadableDatabase, ReadableTable};

use crate::error::{Result, StorageError};
use crate::meta::{Enforcement, IndexField, IndexMeta};
use crate::tables;

/// Guard against a compound index over two array fields producing a
/// combinatorial number of entries. Mongo rejects this outright, and so do we —
/// the alternative is a single document writing millions of index rows.
const MAX_KEYS_PER_DOCUMENT: usize = 1_000;

/// Compute every index key a document contributes.
///
/// Returns more than one key when a field holds an array — a *multikey* index.
/// Both the individual elements and the array as a whole are indexed, so that
/// `{tags: "b"}` and `{tags: ["a","b"]}` are each answerable from the index.
/// Indexing only the elements would leave whole-array equality with no entry,
/// and the planner would return an incomplete result.
pub fn index_keys(index: &IndexMeta, doc: &Document) -> Result<Vec<Vec<u8>>> {
    // Per field, the set of values this document offers.
    let mut per_field: Vec<Vec<Bson>> = Vec::with_capacity(index.fields.len());
    let mut array_fields = 0;

    for field in &index.fields {
        let resolved = path::resolve(doc, &field.path);
        let mut values: Vec<Bson> = Vec::new();

        if resolved.is_empty() {
            // A missing field indexes as null, so `{a: null}` and
            // `{a: {$exists: false}}` remain answerable.
            values.push(Bson::Null);
        } else {
            for value in resolved {
                if let Bson::Array(items) = value {
                    array_fields += 1;
                    values.extend(items.iter().cloned());
                    // ...and the array itself, for whole-array equality.
                    values.push(value.clone());
                } else {
                    values.push(value.clone());
                }
            }
        }

        // Duplicate elements would produce identical keys; drop them early.
        values.dedup_by(|a, b| kimmy_core::canonical_cmp(a, b) == std::cmp::Ordering::Equal);
        per_field.push(values);
    }

    if array_fields > 1 && index.fields.len() > 1 {
        return Err(StorageError::Core(CoreError::InvalidQuery(format!(
            "index {:?} cannot be built for this document: a compound index may span \
             at most one array field",
            index.name
        ))));
    }

    // Cartesian product across fields.
    let mut keys: Vec<Vec<(Bson, bool)>> = vec![Vec::new()];
    for (field, values) in index.fields.iter().zip(per_field) {
        let mut next = Vec::with_capacity(keys.len() * values.len());
        for prefix in &keys {
            for value in &values {
                let mut combined = prefix.clone();
                combined.push((value.clone(), field.descending));
                next.push(combined);
            }
        }
        keys = next;
        if keys.len() > MAX_KEYS_PER_DOCUMENT {
            return Err(StorageError::Core(CoreError::InvalidQuery(format!(
                "index {:?} would produce more than {MAX_KEYS_PER_DOCUMENT} entries \
                 for a single document",
                index.name
            ))));
        }
    }

    let mut encoded: Vec<Vec<u8>> =
        keys.iter().map(|k| keyenc::encode_compound_ordered(k)).collect::<Result<_, _>>()?;
    encoded.sort();
    encoded.dedup();
    Ok(encoded)
}

/// Bring every index on a collection in line with one document write.
///
/// `old` is the document's previous image (`None` for an insert) and `new` its
/// next (`None` for a delete). Called inside the same transaction as the
/// document write, so the index cannot end up describing a state that never
/// existed.
///
/// Unique constraints are checked *before* anything is mutated, so a rejected
/// write leaves the index untouched.
pub(crate) fn maintain(
    txn: &redb::WriteTransaction,
    coll: CollectionId,
    indexes: &[IndexMeta],
    old: Option<&Document>,
    new: Option<&Document>,
    doc_key: &[u8],
) -> Result<()> {
    if indexes.is_empty() {
        return Ok(());
    }
    // One handle for the whole operation: redb refuses to open the same table
    // twice in a transaction, and a `Table` is readable as well as writable.
    let mut table = txn.open_table(tables::INDEX_ENTRIES)?;

    // Check every constraint first. Failing halfway through the mutations
    // would leave the index describing a write that was then rejected.
    if let Some(new) = new {
        for index in indexes.iter().filter(|i| i.unique) {
            for key in index_keys(index, new)? {
                for holder in holders_of(&table, coll, index.id, &key)? {
                    if holder != doc_key {
                        return Err(StorageError::Core(CoreError::DuplicateKey(format!(
                            "unique index {:?}",
                            index.name
                        ))));
                    }
                }
            }
        }
    }

    for index in indexes {
        if let Some(old) = old {
            for key in index_keys(index, old)? {
                table.remove((coll.0, index.id, key.as_slice(), doc_key))?;
            }
        }
        if let Some(new) = new {
            for key in index_keys(index, new)? {
                table.insert((coll.0, index.id, key.as_slice(), doc_key), ())?;
            }
        }
    }
    Ok(())
}

/// Every document id currently filed under one exact index key.
fn holders_of<T>(table: &T, coll: CollectionId, index_id: u32, key: &[u8]) -> Result<Vec<Vec<u8>>>
where
    T: ReadableTable<tables::IndexKey<'static>, ()>,
{
    use std::ops::Bound;
    let start = Bound::Included((coll.0, index_id, key, [].as_slice()));
    let end = Bound::Unbounded;

    let mut out = Vec::new();
    for entry in table.range::<tables::IndexKey<'_>>((start, end))? {
        let (found, _) = entry?;
        let (c, i, k, doc_key) = found.value();
        // The range is open-ended, so stop as soon as we leave this exact key.
        if c != coll.0 || i != index_id || k != key {
            break;
        }
        out.push(doc_key.to_vec());
    }
    Ok(out)
}

/// Scan an index for the document ids under a key range.
///
/// Returns *candidates*: the caller must still apply the full filter. An index
/// narrows the search; only the filter decides membership.
pub(crate) fn scan_range(
    db: &redb::Database,
    coll: CollectionId,
    index_id: u32,
    lower: &[u8],
    upper: Option<&[u8]>,
) -> Result<Vec<Vec<u8>>> {
    use std::ops::Bound;
    let txn = db.begin_read()?;
    let table = txn.open_table(tables::INDEX_ENTRIES)?;

    let start = Bound::Included((coll.0, index_id, lower, [].as_slice()));
    let mut out = Vec::new();
    for entry in table.range::<tables::IndexKey<'_>>((start, Bound::Unbounded))? {
        let (found, _) = entry?;
        let (c, i, k, doc_key) = found.value();
        if c != coll.0 || i != index_id {
            break;
        }
        if let Some(upper) = upper
            && k > upper
        {
            break;
        }
        out.push(doc_key.to_vec());
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Encode a document id the way index entries store it.
pub(crate) fn doc_key_for(id: &DocId) -> Result<Vec<u8>> {
    Ok(keyenc::encode(&id.to_bson())?)
}

impl crate::Engine {
    /// Create an index and populate it from the existing documents.
    ///
    /// The backfill runs inside a single transaction, so the index is either
    /// fully present or entirely absent — a crash partway can never leave a
    /// half-built index, which would silently answer queries with incomplete
    /// results. The cost is that writes to this collection wait for the build.
    pub fn create_index(
        &self,
        db: &str,
        collection: &str,
        fields: Vec<IndexField>,
        unique: bool,
        name: Option<String>,
    ) -> Result<IndexMeta> {
        self.create_index_with(db, collection, fields, unique, Enforcement::Local, name)
    }

    /// Create an index, choosing how far a unique constraint reaches.
    ///
    /// See [`Enforcement`]: `Coordinated` requires cluster machinery that does
    /// not exist yet, so it is refused rather than silently downgraded to a
    /// weaker guarantee than the caller asked for.
    pub fn create_index_with(
        &self,
        db: &str,
        collection: &str,
        fields: Vec<IndexField>,
        unique: bool,
        enforcement: Enforcement,
        name: Option<String>,
    ) -> Result<IndexMeta> {
        if fields.is_empty() {
            return Err(StorageError::Core(CoreError::InvalidQuery(
                "an index needs at least one field".into(),
            )));
        }
        if enforcement == Enforcement::Coordinated {
            return Err(StorageError::Core(CoreError::UnsupportedOperator(
                "coordinated unique enforcement requires clustering, which lands in M4; \
                 use \"local\" enforcement, whose cross-node limits are documented"
                    .into(),
            )));
        }

        let mut meta = self.get_collection(db, collection)?;
        let name = name.unwrap_or_else(|| IndexMeta::default_name(&fields));

        if let Some(existing) = meta.index(&name) {
            // Idempotent when the definition matches, a conflict when it does
            // not — silently keeping the old shape under a reused name would be
            // worse than either.
            if existing.fields == fields && existing.unique == unique {
                return Ok(existing.clone());
            }
            return Err(StorageError::Core(CoreError::CollectionExists {
                db: db.to_string(),
                collection: format!(
                    "{collection}.{name} (index already exists with different fields)"
                ),
            }));
        }

        let index = IndexMeta { id: meta.allocate_index_id(), name, fields, unique, enforcement };

        let txn = self.db().begin_write()?;

        // Scoped in a closure so every table borrow ends before the abort or
        // commit below, which need to move the transaction.
        let build = || -> Result<()> {
            let docs = txn.open_table(tables::DOCS)?;
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            let mut seen_unique: std::collections::HashSet<Vec<u8>> = Default::default();

            for entry in docs.range(crate::engine::doc_range(meta.id))? {
                let (raw_key, raw_value) = entry?;
                let record = crate::codec::decode_doc_record(raw_value.value())?;
                let Some(doc) = record.document()? else { continue };
                let (_, doc_key) = raw_key.value();

                for key in index_keys(&index, &doc)? {
                    // A unique index over data that already violates it must
                    // not be created — it would report a constraint it does
                    // not actually hold.
                    if index.unique && !seen_unique.insert(key.clone()) {
                        return Err(StorageError::Core(CoreError::DuplicateKey(format!(
                            "existing documents violate unique index {:?}",
                            index.name
                        ))));
                    }
                    entries.insert((meta.id.0, index.id, key.as_slice(), doc_key), ())?;
                }
            }
            Ok(())
        };

        if let Err(e) = build() {
            txn.abort()?;
            return Err(e);
        }

        meta.indexes.push(index.clone());
        crate::Engine::put_collection_meta(&txn, &meta)?;
        txn.commit()?;

        tracing::info!(db, collection, index = %index.name, unique, "created index");
        Ok(index)
    }

    /// Drop an index and every entry it holds.
    pub fn drop_index(&self, db: &str, collection: &str, name: &str) -> Result<bool> {
        let mut meta = self.get_collection(db, collection)?;
        let Some(index) = meta.index(name).cloned() else {
            return Ok(false);
        };

        let txn = self.db().begin_write()?;
        {
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            entries.retain_in(index_id_range(meta.id, index.id), |_, _| false)?;
        }
        // The id is not returned to the pool: a future index reusing it would
        // inherit any entry this drop failed to remove.
        meta.indexes.retain(|i| i.name != name);
        crate::Engine::put_collection_meta(&txn, &meta)?;
        txn.commit()?;

        tracing::info!(db, collection, index = name, "dropped index");
        Ok(true)
    }

    pub fn list_indexes(&self, db: &str, collection: &str) -> Result<Vec<IndexMeta>> {
        Ok(self.get_collection(db, collection)?.indexes)
    }

    /// Document keys an index range points at.
    ///
    /// These are **candidates**, not results. An index says which documents
    /// *might* match; the caller must re-apply the full filter.
    pub fn index_candidates(
        &self,
        coll: &crate::CollectionMeta,
        index_id: u32,
        lower: &[u8],
        upper: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        scan_range(self.db(), coll.id, index_id, lower, Some(upper))
    }

    /// Fetch a document by its already-encoded key.
    ///
    /// Index entries store the encoded `_id`, and `keyenc` is one-way — but the
    /// documents table is keyed by that same encoding, so a candidate can be
    /// resolved without ever decoding it.
    pub fn get_by_encoded_key(
        &self,
        coll: &crate::CollectionMeta,
        key: &[u8],
    ) -> Result<Option<Document>> {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        match docs.get((coll.id.0, key))? {
            Some(raw) => Ok(crate::codec::decode_doc_record(raw.value())?.document()?),
            None => Ok(None),
        }
    }
}

/// Key range covering every entry belonging to one index.
fn index_id_range(
    coll: CollectionId,
    index_id: u32,
) -> impl std::ops::RangeBounds<tables::IndexKey<'static>> {
    use std::ops::Bound;
    let start = Bound::Included((coll.0, index_id, [].as_slice(), [].as_slice()));
    let end = match index_id.checked_add(1) {
        Some(next) => Bound::Excluded((coll.0, next, [].as_slice(), [].as_slice())),
        None => Bound::Excluded((coll.0 + 1, 0u32, [].as_slice(), [].as_slice())),
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;
    use crate::meta::IndexField;

    fn index(fields: Vec<IndexField>, unique: bool) -> IndexMeta {
        IndexMeta {
            id: 0,
            name: IndexMeta::default_name(&fields),
            fields,
            unique,
            enforcement: Enforcement::Local,
        }
    }

    fn keys(idx: &IndexMeta, d: Document) -> Vec<Vec<u8>> {
        index_keys(idx, &d).unwrap()
    }

    #[test]
    fn a_scalar_field_produces_one_key() {
        let idx = index(vec![IndexField::ascending("a")], false);
        assert_eq!(keys(&idx, doc! { "a": 1 }).len(), 1);
    }

    #[test]
    fn a_missing_field_indexes_as_null() {
        // Otherwise `{a: null}` — which matches missing fields — would have no
        // index entry to find.
        let idx = index(vec![IndexField::ascending("a")], false);
        let missing = keys(&idx, doc! { "b": 1 });
        let explicit = keys(&idx, doc! { "a": Bson::Null });
        assert_eq!(missing, explicit);
    }

    #[test]
    fn an_array_field_indexes_each_element_and_the_whole_array() {
        let idx = index(vec![IndexField::ascending("tags")], false);
        let k = keys(&idx, doc! { "tags": ["a", "b"] });
        // "a", "b", and ["a","b"] — indexing only the elements would leave
        // whole-array equality unanswerable from the index.
        assert_eq!(k.len(), 3);

        let scalar = keys(&index(vec![IndexField::ascending("tags")], false), doc! { "tags": "a" });
        assert!(k.contains(&scalar[0]), "the element key must match a scalar 'a'");
    }

    #[test]
    fn duplicate_array_elements_collapse() {
        let idx = index(vec![IndexField::ascending("tags")], false);
        // "a", "a", plus the array itself → 2 distinct keys.
        assert_eq!(keys(&idx, doc! { "tags": ["a", "a"] }).len(), 2);
    }

    #[test]
    fn compound_keys_order_by_leading_field() {
        let idx = index(vec![IndexField::ascending("a"), IndexField::ascending("b")], false);
        let low = keys(&idx, doc! { "a": 1, "b": 99 });
        let high = keys(&idx, doc! { "a": 2, "b": 0 });
        assert!(low[0] < high[0], "the leading field must dominate");
    }

    #[test]
    fn a_descending_field_inverts_its_order() {
        let asc = index(vec![IndexField::ascending("a")], false);
        let desc = index(vec![IndexField::descending("a")], false);
        let (a1, a2) = (keys(&asc, doc! { "a": 1 }), keys(&asc, doc! { "a": 2 }));
        let (d1, d2) = (keys(&desc, doc! { "a": 1 }), keys(&desc, doc! { "a": 2 }));
        assert!(a1[0] < a2[0]);
        assert!(d2[0] < d1[0], "descending must reverse");
    }

    #[test]
    fn a_compound_index_over_two_array_fields_is_rejected() {
        // The cartesian product is what makes this dangerous; Mongo rejects it
        // too rather than writing |a| × |b| entries for one document.
        let idx = index(vec![IndexField::ascending("a"), IndexField::ascending("b")], false);
        assert!(index_keys(&idx, &doc! { "a": [1, 2], "b": [3, 4] }).is_err());
        // One array field is fine.
        assert!(index_keys(&idx, &doc! { "a": [1, 2], "b": 3 }).is_ok());
    }

    #[test]
    fn nested_paths_are_indexed() {
        let idx = index(vec![IndexField::ascending("addr.city")], false);
        let nested = keys(&idx, doc! { "addr": { "city": "berlin" } });
        let flat = keys(&index(vec![IndexField::ascending("c")], false), doc! { "c": "berlin" });
        assert_eq!(nested[0], flat[0], "the value encoding must not depend on the path");
    }

    #[test]
    fn equal_values_across_numeric_types_share_a_key() {
        // An index lookup for 5 must find a document that stored 5.0.
        let idx = index(vec![IndexField::ascending("n")], false);
        assert_eq!(keys(&idx, doc! { "n": 5i32 }), keys(&idx, doc! { "n": 5.0 }));
        assert_eq!(keys(&idx, doc! { "n": 5i64 }), keys(&idx, doc! { "n": 5.0 }));
    }

    // -----------------------------------------------------------------------
    // Lifecycle and maintenance, against a real engine
    // -----------------------------------------------------------------------

    use crate::{CollectionMeta, Engine};

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let c = e.create_collection("app", "docs").unwrap();
        (e, c, dir)
    }

    /// Every document id currently filed under an index, via a full scan of it.
    fn entries_for(engine: &Engine, coll: &CollectionMeta, index_id: u32) -> Vec<Vec<u8>> {
        scan_range(engine.db(), coll.id, index_id, &[], None).unwrap()
    }

    #[test]
    fn creating_an_index_backfills_existing_documents() {
        let (engine, coll, _dir) = engine();
        for i in 1..=3 {
            engine.insert(&coll, doc! { "_id": i, "qty": i * 10 }).unwrap();
        }

        let idx = engine
            .create_index("app", "docs", vec![IndexField::ascending("qty")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        assert_eq!(entries_for(&engine, &coll, idx.id).len(), 3);
    }

    #[test]
    fn writes_after_creation_maintain_the_index() {
        let (engine, _coll, _dir) = engine();
        let idx = engine
            .create_index("app", "docs", vec![IndexField::ascending("qty")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        engine.insert(&coll, doc! { "_id": 1, "qty": 5 }).unwrap();
        assert_eq!(entries_for(&engine, &coll, idx.id).len(), 1);

        // A replace must remove the old key, not merely add the new one.
        engine.replace(&coll, &DocId::Int64(1), doc! { "qty": 99 }, false).unwrap();
        assert_eq!(entries_for(&engine, &coll, idx.id).len(), 1, "stale entry left behind");

        // A delete must leave nothing pointing at a document that is gone.
        engine.delete(&coll, &DocId::Int64(1)).unwrap();
        assert!(entries_for(&engine, &coll, idx.id).is_empty());
    }

    #[test]
    fn a_unique_index_rejects_a_duplicate_write() {
        let (engine, _coll, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("email")], true, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        engine.insert(&coll, doc! { "_id": 1, "email": "a@x.com" }).unwrap();
        let err = engine.insert(&coll, doc! { "_id": 2, "email": "a@x.com" });
        assert!(matches!(err, Err(StorageError::Core(CoreError::DuplicateKey(_)))));

        // The rejected write must leave nothing behind — neither document nor
        // index entry.
        assert!(engine.get(&coll, &DocId::Int64(2)).unwrap().is_none());
        assert_eq!(engine.count(&coll).unwrap(), 1);
    }

    #[test]
    fn a_unique_index_allows_updating_a_document_in_place() {
        // The document's own existing entry must not count as a conflict.
        let (engine, _coll, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("email")], true, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        engine.insert(&coll, doc! { "_id": 1, "email": "a@x.com" }).unwrap();
        engine
            .replace(&coll, &DocId::Int64(1), doc! { "email": "a@x.com", "n": 1 }, false)
            .unwrap();
        assert_eq!(engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_i32("n").unwrap(), 1);
    }

    #[test]
    fn a_unique_index_is_refused_when_existing_data_violates_it() {
        // Creating it anyway would advertise a constraint that does not hold.
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1, "email": "a@x.com" }).unwrap();
        engine.insert(&coll, doc! { "_id": 2, "email": "a@x.com" }).unwrap();

        let err =
            engine.create_index("app", "docs", vec![IndexField::ascending("email")], true, None);
        assert!(matches!(err, Err(StorageError::Core(CoreError::DuplicateKey(_)))));
        // ...and the failed build must leave no index registered.
        assert!(engine.list_indexes("app", "docs").unwrap().is_empty());
    }

    #[test]
    fn dropping_an_index_removes_its_entries_but_not_others() {
        let (engine, _coll, _dir) = engine();
        let a = engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, None)
            .unwrap();
        let b = engine
            .create_index("app", "docs", vec![IndexField::ascending("b")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        engine.insert(&coll, doc! { "_id": 1, "a": 1, "b": 2 }).unwrap();

        assert!(engine.drop_index("app", "docs", &a.name).unwrap());
        assert!(entries_for(&engine, &coll, a.id).is_empty());
        assert_eq!(entries_for(&engine, &coll, b.id).len(), 1, "the other index must survive");
        assert!(!engine.drop_index("app", "docs", &a.name).unwrap());
    }

    #[test]
    fn a_dropped_index_id_is_never_reissued() {
        // A reused id would inherit any entry the drop failed to remove.
        let (engine, _coll, _dir) = engine();
        let a = engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, None)
            .unwrap();
        engine.drop_index("app", "docs", &a.name).unwrap();
        let b = engine
            .create_index("app", "docs", vec![IndexField::ascending("b")], false, None)
            .unwrap();
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn creating_the_same_index_twice_is_idempotent() {
        let (engine, _coll, _dir) = engine();
        let fields = vec![IndexField::ascending("a")];
        let first = engine.create_index("app", "docs", fields.clone(), false, None).unwrap();
        let again = engine.create_index("app", "docs", fields, false, None).unwrap();
        assert_eq!(first.id, again.id);
        assert_eq!(engine.list_indexes("app", "docs").unwrap().len(), 1);
    }

    #[test]
    fn reusing_a_name_for_a_different_definition_is_a_conflict() {
        let (engine, _coll, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, Some("i".into()))
            .unwrap();
        assert!(
            engine
                .create_index(
                    "app",
                    "docs",
                    vec![IndexField::ascending("b")],
                    false,
                    Some("i".into())
                )
                .is_err()
        );
    }

    #[test]
    fn indexes_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let id = {
            let e = Engine::open(&path).unwrap();
            let c = e.create_collection("app", "docs").unwrap();
            e.insert(&c, doc! { "_id": 1, "a": 7 }).unwrap();
            e.create_index("app", "docs", vec![IndexField::ascending("a")], false, None).unwrap().id
        };

        let e = Engine::open(&path).unwrap();
        let c = e.get_collection("app", "docs").unwrap();
        assert_eq!(e.list_indexes("app", "docs").unwrap().len(), 1);
        assert_eq!(entries_for(&e, &c, id).len(), 1);
    }

    #[test]
    fn an_empty_field_list_is_rejected() {
        let (engine, _coll, _dir) = engine();
        assert!(engine.create_index("app", "docs", vec![], false, None).is_err());
    }

    #[test]
    fn coordinated_enforcement_is_refused_until_clustering_exists() {
        // Silently downgrading to a weaker guarantee than the caller asked for
        // would be worse than failing.
        let (engine, _coll, _dir) = engine();
        let err = engine.create_index_with(
            "app",
            "docs",
            vec![IndexField::ascending("email")],
            true,
            Enforcement::Coordinated,
            None,
        );
        assert!(matches!(err, Err(StorageError::Core(CoreError::UnsupportedOperator(_)))));
    }

    // -----------------------------------------------------------------------
    // The invariant the whole feature rests on
    // -----------------------------------------------------------------------

    /// Documents matching a filter, found by a full collection scan.
    fn by_scan(engine: &Engine, coll: &CollectionMeta, query: &Document) -> Vec<i64> {
        let filter = kimmy_query::filter::parse(query).unwrap();
        let mut ids = Vec::new();
        engine
            .for_each_doc(coll, |id, doc| {
                if kimmy_query::filter::matches(&filter, &doc)
                    && let DocId::Int64(n) = id
                {
                    ids.push(n);
                }
                Ok(true)
            })
            .unwrap();
        ids.sort_unstable();
        ids
    }

    /// The same, found through whichever index the planner chooses.
    ///
    /// Returns `None` when no index applies, so the caller can tell "the index
    /// path agreed" from "the index path never ran".
    fn by_index(engine: &Engine, coll: &CollectionMeta, query: &Document) -> Option<Vec<i64>> {
        let filter = kimmy_query::filter::parse(query).unwrap();
        let plan = kimmy_query::plan::choose(&filter, &coll.indexes)?;

        let candidates =
            engine.index_candidates(coll, plan.index_id, &plan.lower, &plan.upper).unwrap();

        let mut ids = Vec::new();
        for key in candidates {
            // The recheck. An index narrows; only the filter decides.
            if let Some(doc) = engine.get_by_encoded_key(coll, &key).unwrap()
                && kimmy_query::filter::matches(&filter, &doc)
                && let Ok(DocId::Int64(n)) = DocId::try_from_bson(doc.get("_id").unwrap())
            {
                ids.push(n);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        Some(ids)
    }

    /// A dataset chosen to exercise the cases an index most easily gets wrong:
    /// missing fields, nulls, arrays, mixed numeric types, and duplicates.
    fn seeded() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let (engine, coll, dir) = engine();
        let docs = vec![
            doc! { "_id": 1i64, "a": 1, "n": 10, "tags": ["x", "y"] },
            doc! { "_id": 2i64, "a": 1, "n": 20, "tags": ["y"] },
            doc! { "_id": 3i64, "a": 2, "n": 10, "tags": [] },
            doc! { "_id": 4i64, "a": 2, "n": 30 },
            doc! { "_id": 5i64, "a": Bson::Null, "n": 10 },
            doc! { "_id": 6i64, "n": 40, "tags": "x" },
            doc! { "_id": 7i64, "a": 1.0, "n": 10.0 },
            doc! { "_id": 8i64, "a": 1, "n": 20, "tags": ["x", "x"] },
        ];
        for d in docs {
            engine.insert(&coll, d).unwrap();
        }
        (engine, coll, dir)
    }

    #[test]
    fn index_backed_results_are_identical_to_a_full_scan() {
        let (engine, _coll, _dir) = seeded();

        // Build several indexes over the same data so the planner has choices.
        for fields in [
            vec![IndexField::ascending("a")],
            vec![IndexField::ascending("n")],
            vec![IndexField::ascending("a"), IndexField::ascending("n")],
            vec![IndexField::ascending("tags")],
            vec![IndexField::descending("n")],
        ] {
            engine.create_index("app", "docs", fields, false, None).unwrap();
        }
        let coll = engine.get_collection("app", "docs").unwrap();

        let queries = vec![
            doc! { "a": 1 },
            doc! { "a": 1.0 },
            doc! { "a": 2 },
            doc! { "a": Bson::Null },
            doc! { "a": 999 },
            doc! { "n": { "$gt": 15 } },
            doc! { "n": { "$gte": 10, "$lt": 30 } },
            doc! { "n": { "$lte": 10 } },
            doc! { "a": 1, "n": 20 },
            doc! { "a": 1, "n": { "$gt": 15 } },
            doc! { "a": 2, "n": { "$lt": 100 } },
            doc! { "tags": "x" },
            doc! { "tags": "y" },
            doc! { "tags": ["x", "y"] },
            doc! { "a": 1, "$or": [ { "n": 10 }, { "n": 20 } ] },
            doc! { "a": 1, "n": { "$ne": 20 } },
            doc! { "a": 1, "tags": "x" },
        ];

        let mut exercised = 0;
        for query in &queries {
            let scan = by_scan(&engine, &coll, query);
            if let Some(indexed) = by_index(&engine, &coll, query) {
                assert_eq!(
                    indexed, scan,
                    "index and scan disagree for {query:?} — the index path is wrong"
                );
                exercised += 1;
            }
        }
        assert!(
            exercised >= 12,
            "only {exercised} queries used an index; test is not proving much"
        );
    }

    /// A descending index must give the same answers as a scan.
    ///
    /// It is the *only* index here, so the planner cannot quietly sidestep it
    /// by preferring an ascending one — which is exactly what hid a planner
    /// bug from the broader test above until mutation testing found it.
    #[test]
    fn a_descending_index_agrees_with_a_scan_including_two_sided_ranges() {
        let (engine, _coll, _dir) = seeded();
        engine.create_index("app", "docs", vec![IndexField::descending("n")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        for query in [
            doc! { "n": 10 },
            doc! { "n": { "$gt": 15 } },
            doc! { "n": { "$lte": 10 } },
            // Two-sided: a bound encoded in the wrong direction makes this
            // range empty rather than merely wide.
            doc! { "n": { "$gte": 10, "$lte": 30 } },
            doc! { "n": { "$gt": 10, "$lt": 40 } },
        ] {
            let scan = by_scan(&engine, &coll, &query);
            assert!(!scan.is_empty(), "{query:?} should match something, or it proves nothing");
            if let Some(indexed) = by_index(&engine, &coll, &query) {
                assert_eq!(indexed, scan, "descending index disagreed for {query:?}");
            }
        }
    }

    #[test]
    fn index_backed_results_stay_correct_as_documents_change() {
        // Maintenance bugs surface as stale entries, which show up as an index
        // result that no longer matches the scan.
        let (engine, _coll, _dir) = seeded();
        engine.create_index("app", "docs", vec![IndexField::ascending("a")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        let check = |label: &str| {
            for query in [doc! { "a": 1 }, doc! { "a": 2 }, doc! { "a": 7 }] {
                let scan = by_scan(&engine, &coll, &query);
                if let Some(indexed) = by_index(&engine, &coll, &query) {
                    assert_eq!(indexed, scan, "{label}: disagreement for {query:?}");
                }
            }
        };

        check("initial");
        engine.replace(&coll, &DocId::Int64(1), doc! { "a": 7 }, false).unwrap();
        check("after replace");
        engine.delete(&coll, &DocId::Int64(2)).unwrap();
        check("after delete");
        engine.insert(&coll, doc! { "_id": 99i64, "a": 1 }).unwrap();
        check("after insert");
        engine.delete(&coll, &DocId::Int64(3)).unwrap();
        engine.insert(&coll, doc! { "_id": 3i64, "a": 2 }).unwrap();
        check("after delete and reinsert");
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            // Each case builds a real engine, so the count is modest; the
            // deterministic matrix above carries the breadth.
            #![proptest_config(ProptestConfig::with_cases(48))]

            /// For any dataset and any equality or range filter, going through
            /// the index must return exactly what a full scan returns.
            #[test]
            fn an_index_never_changes_a_query_result(
                values in prop::collection::vec(
                    prop_oneof![
                        Just(Bson::Null),
                        (0i32..5).prop_map(Bson::Int32),
                        (0i64..5).prop_map(|n| Bson::Double(n as f64)),
                        prop::collection::vec(0i32..3, 0..3)
                            .prop_map(|v| Bson::Array(v.into_iter().map(Bson::Int32).collect())),
                    ],
                    1..12,
                ),
                probe in 0i32..5,
                // A two-sided range matters: with only a lower bound, a
                // mis-encoded bound makes the range too *wide*, which the
                // recheck silently repairs. Both ends are needed to expose a
                // range that is too narrow.
                span in 0i32..4,
                shape in 0u8..3,
                descending in any::<bool>(),
            ) {
                let (engine, coll, _dir) = engine();
                for (i, v) in values.iter().enumerate() {
                    engine.insert(&coll, doc! { "_id": i as i64, "a": v.clone() }).unwrap();
                }
                let field = if descending {
                    IndexField::descending("a")
                } else {
                    IndexField::ascending("a")
                };
                engine.create_index("app", "docs", vec![field], false, None).unwrap();
                let coll = engine.get_collection("app", "docs").unwrap();

                let query = match shape {
                    0 => doc! { "a": probe },
                    1 => doc! { "a": { "$gte": probe } },
                    // Two-sided: the shape that catches a too-narrow range.
                    _ => doc! { "a": { "$gte": probe, "$lte": probe + span } },
                };

                let scan = by_scan(&engine, &coll, &query);
                if let Some(indexed) = by_index(&engine, &coll, &query) {
                    prop_assert_eq!(indexed, scan, "index disagreed with scan for {:?}", query);
                }
            }
        }
    }
}
