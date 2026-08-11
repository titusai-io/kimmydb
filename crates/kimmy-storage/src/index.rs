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
    Ok(index_keys_observed(index, doc)?.0)
}

/// [`index_keys`], also reporting whether this document makes the index
/// multikey.
///
/// Multikey means some field contributed more than one value: it held an
/// array, or its path fanned out through one (`a.b` over `{a: [{b: 1},
/// {b: 2}]}`). That is the condition under which a two-sided key range stops
/// being sound, so it is what the write path records — see
/// [`IndexMeta::multikey`].
pub(crate) fn index_keys_observed(
    index: &IndexMeta,
    doc: &Document,
) -> Result<(Vec<Vec<u8>>, bool)> {
    // Per field, the set of values this document offers.
    let mut per_field: Vec<Vec<Bson>> = Vec::with_capacity(index.fields.len());
    let mut array_fields = 0;
    let mut multikey = false;

    for field in &index.fields {
        let resolved = path::resolve(doc, &field.path);
        let mut values: Vec<Bson> = Vec::new();

        // A path that resolves to several values has fanned out through an
        // array of documents — multikey even though no value is itself an
        // array.
        multikey |= resolved.len() > 1;

        if resolved.is_empty() {
            // A missing field indexes as null, so `{a: null}` and
            // `{a: {$exists: false}}` remain answerable.
            values.push(Bson::Null);
        } else {
            for value in resolved {
                if let Bson::Array(items) = value {
                    array_fields += 1;
                    multikey = true;
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
    Ok((encoded, multikey))
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
///
/// Returns the ids of indexes this write has just made multikey, which the
/// caller must persist with [`mark_multikey`] **in the same transaction** — a
/// flag committed later than the entries would leave a window in which the
/// planner intersects a two-sided range over an index that already holds an
/// array's keys.
pub(crate) fn maintain(
    txn: &redb::WriteTransaction,
    coll: &crate::CollectionMeta,
    old: Option<&Document>,
    new: Option<&Document>,
    doc_key: &[u8],
) -> Result<Vec<u32>> {
    let indexes = current_indexes(txn, coll)?;
    if indexes.is_empty() {
        return Ok(Vec::new());
    }
    // One handle for the whole operation: redb refuses to open the same table
    // twice in a transaction, and a `Table` is readable as well as writable.
    let mut table = txn.open_table(tables::INDEX_ENTRIES)?;

    // Check every constraint first. Failing halfway through the mutations
    // would leave the index describing a write that was then rejected.
    if let Some(new) = new {
        for index in indexes.iter().filter(|i| i.unique) {
            for key in index_keys(index, new)? {
                for holder in holders_of(&table, coll.id, index.id, &key)? {
                    if holder != doc_key {
                        return Err(StorageError::Core(CoreError::UniqueViolation {
                            index: index.name.clone(),
                            detail: "another document already holds this value".into(),
                        }));
                    }
                }
            }
        }
    }

    apply_entries(&mut table, coll.id, &indexes, old, new, doc_key)
}

/// The index definitions as this transaction sees them.
///
/// Read here rather than trusted from the caller, because the caller's
/// `CollectionMeta` was fetched in an *earlier* transaction. An index created
/// in between would be silently skipped — no entries for this write, a unique
/// constraint never checked, an array never observed — and the write path is
/// the one place that can notice, since write transactions serialize. The
/// caller's handle still names the collection; only the definitions are
/// re-read.
fn current_indexes(
    txn: &redb::WriteTransaction,
    coll: &crate::CollectionMeta,
) -> Result<Vec<IndexMeta>> {
    let collections = txn.open_table(tables::COLLECTIONS)?;
    Ok(match collections.get((coll.db.as_str(), coll.name.as_str()))? {
        Some(raw) => serde_json::from_slice::<crate::CollectionMeta>(raw.value())?.indexes,
        // Not stored: the collection is being created or restored in this very
        // transaction, and the caller's copy is the only truth there is.
        None => coll.indexes.clone(),
    })
}

/// A unique constraint that a merged write broke.
///
/// Produced only by [`maintain_remote`]. A local write is *rejected* on
/// violation, so there is nothing to report; a replicated one cannot be
/// rejected without abandoning convergence, so it is recorded instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueViolation {
    pub index: String,
    /// Encoded index key that now has more than one holder.
    pub key: Vec<u8>,
    /// Encoded document keys holding it, including the one just applied.
    pub holders: Vec<Vec<u8>>,
}

/// Maintain indexes for a *replicated* write, reporting rather than rejecting.
///
/// The asymmetry with [`maintain`] is deliberate and is the whole of
/// [ADR-020](../../../docs/decisions.md). A local write can be refused because
/// the client is still there to be told. A replicated write cannot: refusing it
/// means the two nodes never agree, which trades away the availability this
/// design exists to provide. Uniqueness is not I-confluent — no merge function
/// can repair it — so the only honest options are to converge with a violated
/// constraint or to diverge, and diverging is worse.
///
/// So the entry goes in regardless and the collision is returned. Adding the
/// entry rather than skipping it matters: a missing entry would leave an
/// index-backed query silently unable to find a document that exists, which is
/// a wrong answer rather than a reported problem.
pub(crate) fn maintain_remote(
    txn: &redb::WriteTransaction,
    coll: &crate::CollectionMeta,
    old: Option<&Document>,
    new: Option<&Document>,
    doc_key: &[u8],
) -> Result<(Vec<UniqueViolation>, Vec<u32>)> {
    let indexes = current_indexes(txn, coll)?;
    if indexes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut table = txn.open_table(tables::INDEX_ENTRIES)?;

    let mut violations = Vec::new();
    if let Some(new) = new {
        for index in indexes.iter().filter(|i| i.unique) {
            for key in index_keys(index, new)? {
                let mut holders: Vec<Vec<u8>> = holders_of(&table, coll.id, index.id, &key)?
                    .into_iter()
                    .filter(|holder| holder != doc_key)
                    .collect();
                if !holders.is_empty() {
                    holders.push(doc_key.to_vec());
                    violations.push(UniqueViolation {
                        index: index.name.clone(),
                        key: key.clone(),
                        holders,
                    });
                }
            }
        }
    }

    let newly_multikey = apply_entries(&mut table, coll.id, &indexes, old, new, doc_key)?;
    Ok((violations, newly_multikey))
}

/// Remove the old image's entries and add the new one's.
///
/// Returns the ids of indexes the **new** image has just made multikey — those
/// where it contributed more than one key and the definition does not say so
/// yet. The old image is not consulted: the flag is one-way, so only the state
/// being written can flip it.
fn apply_entries(
    table: &mut redb::Table<'_, tables::IndexKey<'static>, ()>,
    coll: CollectionId,
    indexes: &[IndexMeta],
    old: Option<&Document>,
    new: Option<&Document>,
    doc_key: &[u8],
) -> Result<Vec<u32>> {
    let mut newly_multikey = Vec::new();
    for index in indexes {
        if let Some(old) = old {
            for key in index_keys(index, old)? {
                table.remove((coll.0, index.id, key.as_slice(), doc_key))?;
            }
        }
        if let Some(new) = new {
            let (keys, multikey) = index_keys_observed(index, new)?;
            if multikey && !index.multikey {
                newly_multikey.push(index.id);
            }
            for key in keys {
                table.insert((coll.0, index.id, key.as_slice(), doc_key), ())?;
            }
        }
    }
    Ok(newly_multikey)
}

/// Persist that these indexes are now multikey, in the caller's transaction.
///
/// Re-reads the definition through the transaction rather than trusting the
/// caller's copy: the copy predates the transaction, and writing it back would
/// resurrect anything that changed in between. Setting a flag that is already
/// set is a harmless no-op, which is what makes racing observers safe.
pub(crate) fn mark_multikey(
    txn: &redb::WriteTransaction,
    db: &str,
    collection: &str,
    index_ids: &[u32],
) -> Result<()> {
    if index_ids.is_empty() {
        return Ok(());
    }
    let mut collections = txn.open_table(tables::COLLECTIONS)?;
    let mut meta: crate::CollectionMeta = match collections.get((db, collection))? {
        Some(raw) => serde_json::from_slice(raw.value())?,
        // Gone mid-transaction cannot happen — writes are serialized — but a
        // missing definition is not worth failing the document write over.
        None => return Ok(()),
    };
    for index in meta.indexes.iter_mut() {
        if index_ids.contains(&index.id) {
            index.multikey = true;
        }
    }
    collections.insert((db, collection), serde_json::to_vec(&meta)?.as_slice())?;
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
    scan_range_in(&db.begin_read()?, coll, index_id, lower, upper)
}

/// [`scan_range`] inside a caller-held transaction, for scans that must share
/// a snapshot with something else — see
/// [`crate::Engine::index_candidates_unless_multikey`].
fn scan_range_in(
    txn: &redb::ReadTransaction,
    coll: CollectionId,
    index_id: u32,
    lower: &[u8],
    upper: Option<&[u8]>,
) -> Result<Vec<Vec<u8>>> {
    use std::ops::Bound;
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
        self.create_index_inner(db, collection, fields, unique, enforcement, name, true)
    }

    /// `log = false` when applying a replicated definition.
    ///
    /// A replicated change must not mint an entry of its own: the originating
    /// entry is appended by the caller, and minting a second one under this
    /// node's stamp would send the same change back to the peer, which would
    /// apply it and mint another. That amplifies without bound.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_index_inner(
        &self,
        db: &str,
        collection: &str,
        fields: Vec<IndexField>,
        unique: bool,
        enforcement: Enforcement,
        name: Option<String>,
        log: bool,
    ) -> Result<IndexMeta> {
        if fields.is_empty() {
            return Err(StorageError::Core(CoreError::InvalidQuery(
                "an index needs at least one field".into(),
            )));
        }
        if enforcement == Enforcement::Coordinated {
            return Err(StorageError::Core(CoreError::Unsupported(
                "coordinated unique enforcement is reserved and not implemented; it needs \
                 value-ownership routing, which trades availability for the guarantee. Use \
                 \"local\" enforcement, whose cross-node limits are documented"
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

        // Derived from the name so every node agrees, which is what lets an
        // index definition replicate at all.
        let id = IndexMeta::derive_id(&name);
        if let Some(existing) = meta.index_by_id(id) {
            return Err(StorageError::Corrupt(format!(
                "index id for {name:?} collides with existing index {:?} on {db}.{collection}; \
                 rename one of them",
                existing.name
            )));
        }

        let mut index = IndexMeta { id, name, fields, unique, enforcement, multikey: false };

        let txn = self.db().begin_write()?;

        // Scoped in a closure so every table borrow ends before the abort or
        // commit below, which need to move the transaction. Returns whether the
        // existing documents already make the index multikey — the backfill is
        // the flag's only chance to see them.
        let build = |index: &IndexMeta| -> Result<bool> {
            let docs = txn.open_table(tables::DOCS)?;
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            let mut seen_unique: std::collections::HashSet<Vec<u8>> = Default::default();
            let mut observed_multikey = false;

            for entry in docs.range(crate::engine::doc_range(meta.id))? {
                let (raw_key, raw_value) = entry?;
                let record = crate::codec::decode_doc_record(raw_value.value())?;
                let Some(doc) = record.document()? else { continue };
                let (_, doc_key) = raw_key.value();

                let (keys, multikey) = index_keys_observed(index, &doc)?;
                observed_multikey |= multikey;
                for key in keys {
                    // A unique index over data that already violates it must
                    // not be created — it would report a constraint it does
                    // not actually hold.
                    if index.unique && !seen_unique.insert(key.clone()) {
                        return Err(StorageError::Core(CoreError::UniqueViolation {
                            index: index.name.clone(),
                            detail: "existing documents already violate it, so it cannot be \
                                     created"
                                .into(),
                        }));
                    }
                    entries.insert((meta.id.0, index.id, key.as_slice(), doc_key), ())?;
                }
            }
            Ok(observed_multikey)
        };

        match build(&index) {
            Ok(observed) => index.multikey = observed,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        }

        meta.indexes.push(index.clone());
        crate::Engine::put_collection_meta(&txn, &meta)?;

        let logged = if log {
            let entry = crate::engine::ddl_entry(
                self.next_stamp(),
                kimmy_core::OpKind::CreateIndex,
                meta.id,
                &kimmy_core::IndexCreate {
                    db: db.to_string(),
                    collection: collection.to_string(),
                    index: index.clone(),
                },
            )?;
            crate::engine::append_oplog(&txn, &entry)?;
            Some(entry)
        } else {
            None
        };
        txn.commit()?;
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

        tracing::info!(db, collection, index = %index.name, unique, "created index");
        Ok(index)
    }

    /// Drop an index and every entry it holds.
    pub fn drop_index(&self, db: &str, collection: &str, name: &str) -> Result<bool> {
        self.drop_index_inner(db, collection, name, true)
    }

    pub(crate) fn drop_index_inner(
        &self,
        db: &str,
        collection: &str,
        name: &str,
        log: bool,
    ) -> Result<bool> {
        let mut meta = self.get_collection(db, collection)?;
        let Some(index) = meta.index(name).cloned() else {
            return Ok(false);
        };

        let txn = self.db().begin_write()?;
        {
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            entries.retain_in(index_id_range(meta.id, index.id), |_, _| false)?;
        }
        // Entries are removed above, in this same transaction, which is what
        // makes it safe for an index later recreated under the same name to
        // receive the same derived id — it cannot inherit anything.
        meta.indexes.retain(|i| i.name != name);
        crate::Engine::put_collection_meta(&txn, &meta)?;

        let logged = if log {
            let entry = crate::engine::ddl_entry(
                self.next_stamp(),
                kimmy_core::OpKind::DropIndex,
                meta.id,
                &kimmy_core::IndexDrop {
                    db: db.to_string(),
                    collection: collection.to_string(),
                    index: name.to_string(),
                },
            )?;
            crate::engine::append_oplog(&txn, &entry)?;
            Some(entry)
        } else {
            None
        };
        txn.commit()?;
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

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

    /// Candidates for a range that is only sound while the index is **not**
    /// multikey. `None` means the caller must re-plan.
    ///
    /// A two-sided range intersects both bounds, which loses documents once
    /// any of them contributes several keys — and the plan was built from
    /// metadata read in an *earlier* transaction. A write between that read
    /// and this scan could have made the index multikey. So the flag is
    /// re-read here, **in the same transaction as the scan**: what this
    /// snapshot's flag approves is sound for exactly this snapshot's entries.
    /// A `false` from a previous snapshot proves nothing about this one.
    pub fn index_candidates_unless_multikey(
        &self,
        coll: &crate::CollectionMeta,
        index_id: u32,
        lower: &[u8],
        upper: &[u8],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let txn = self.db().begin_read()?;
        {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            let fresh: crate::CollectionMeta =
                match collections.get((coll.db.as_str(), coll.name.as_str()))? {
                    Some(raw) => serde_json::from_slice(raw.value())?,
                    // Dropped since the plan was built. This same snapshot
                    // holds no documents either, so empty is the truth.
                    None => return Ok(Some(Vec::new())),
                };
            match fresh.index_by_id(index_id) {
                Some(index) if !index.multikey => {}
                // Multikey now, or the index is gone: the plan's bounds no
                // longer mean what they meant.
                _ => return Ok(None),
            }
        }
        scan_range_in(&txn, coll.id, index_id, lower, Some(upper)).map(Some)
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
            multikey: false,
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
        assert!(matches!(err, Err(StorageError::Core(CoreError::UniqueViolation { .. }))));

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
        assert!(matches!(err, Err(StorageError::Core(CoreError::UniqueViolation { .. }))));
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
        assert!(matches!(err, Err(StorageError::Core(CoreError::Unsupported(_)))));
    }

    // -----------------------------------------------------------------------
    // Multikey tracking: what licenses a two-sided range
    // -----------------------------------------------------------------------

    fn multikey_of(engine: &Engine, name: &str) -> bool {
        engine
            .get_collection("app", "docs")
            .unwrap()
            .indexes
            .iter()
            .find(|i| i.name == name)
            .unwrap()
            .multikey
    }

    #[test]
    fn an_array_write_marks_the_index_multikey() {
        // The write-path half of the flag: the index exists first, and the
        // array arrives later. Scalar writes must not set it — the flag's
        // whole value is staying false for the scalar-only majority.
        let (engine, _coll, _dir) = engine();
        let idx = engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        engine.insert(&coll, doc! { "_id": 1, "a": 5 }).unwrap();
        assert!(!multikey_of(&engine, &idx.name), "a scalar write must not set the flag");

        engine.insert(&coll, doc! { "_id": 2, "a": [1, 2] }).unwrap();
        assert!(multikey_of(&engine, &idx.name), "an array write must set it");

        // One-way: deleting the only array document does not clear it, because
        // nothing proves no other document holds one without a full scan.
        engine.delete(&coll, &DocId::Int64(2)).unwrap();
        assert!(multikey_of(&engine, &idx.name), "the flag never clears");
    }

    #[test]
    fn backfill_marks_an_index_multikey_when_arrays_already_exist() {
        // The other half: the documents exist first. The backfill is the
        // flag's only chance to see them.
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1, "a": [1, 2] }).unwrap();
        let idx = engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, None)
            .unwrap();
        assert!(idx.multikey, "the backfill saw an array");
        assert!(multikey_of(&engine, &idx.name), "and the stored definition agrees");
    }

    #[test]
    fn a_path_fanning_out_through_an_array_is_multikey() {
        // `a.b` over `{a: [{b: 1}, {b: 2}]}` contributes two keys without any
        // indexed value being an array itself. Same hazard, same flag.
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1, "a": [ { "b": 1 }, { "b": 2 } ] }).unwrap();
        let idx = engine
            .create_index("app", "docs", vec![IndexField::ascending("a.b")], false, None)
            .unwrap();
        assert!(idx.multikey, "path fan-out is multikey even with no array value");
    }

    #[test]
    fn the_multikey_flag_survives_a_restart() {
        // It is part of the persisted definition, not a runtime observation —
        // a restart that forgot it would resume intersecting ranges over an
        // index that holds an array's keys.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let e = Engine::open(&path).unwrap();
            e.create_collection("app", "docs").unwrap();
            e.create_index("app", "docs", vec![IndexField::ascending("a")], false, None).unwrap();
            // Re-fetched so the write sees the index; a stale handle would
            // maintain nothing.
            let c = e.get_collection("app", "docs").unwrap();
            e.insert(&c, doc! { "_id": 1, "a": [1, 2] }).unwrap();
        }
        let e = Engine::open(&path).unwrap();
        assert!(multikey_of(&e, "a_1"));
    }

    #[test]
    fn a_replicated_array_write_marks_the_index_multikey() {
        // The flag is a node-local observation, so the node applying a peer's
        // write must make it too — its own planner answers queries over the
        // merged data.
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a = Engine::open(&a_dir.path().join("kimmy.redb")).unwrap();
        let b = Engine::open(&b_dir.path().join("kimmy.redb")).unwrap();
        for engine in [&a, &b] {
            engine.create_collection("app", "docs").unwrap();
            engine
                .create_index("app", "docs", vec![IndexField::ascending("a")], false, None)
                .unwrap();
        }

        let coll = a.get_collection("app", "docs").unwrap();
        a.insert(&coll, doc! { "_id": 1, "a": [1, 2] }).unwrap();
        let entries = a.entries_for_peer(kimmy_core::Hlc::ZERO, 100).unwrap();
        b.apply_batch(&entries).unwrap();

        assert!(multikey_of(&b, "a_1"), "the applying node must observe what it applied");
    }

    #[test]
    fn a_two_sided_range_stays_correct_when_arrays_arrive_after_the_index() {
        // The order the backfill cannot cover: the index watches the arrays
        // arrive through the write path. If the flag failed to flip, the
        // planner would intersect both bounds over an index where different
        // elements satisfy each one, and _id 1 would silently vanish from the
        // result.
        let (engine, _coll, _dir) = engine();
        engine.create_index("app", "docs", vec![IndexField::ascending("a")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        engine.insert(&coll, doc! { "_id": 1i64, "a": [2, 0] }).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64, "a": 1 }).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        for query in
            [doc! { "a": { "$gte": 1, "$lte": 1 } }, doc! { "a": { "$gte": 0, "$lte": 2 } }]
        {
            let scan = by_scan(&engine, &coll, &query);
            let indexed = by_index(&engine, &coll, &query).expect("the index should apply");
            assert_eq!(indexed, scan, "index lost documents for {query:?}");
        }
    }

    #[test]
    fn a_two_sided_range_on_a_scalar_only_index_reads_only_the_range() {
        // The selectivity the flag buys back — and the proof the register's
        // one red drift is closed. Twenty scalar documents, a range covering
        // five: the scan must touch five candidates, not everything from the
        // lower bound up.
        let (engine, coll, _dir) = engine();
        for i in 0..20 {
            engine.insert(&coll, doc! { "_id": i as i64, "n": i }).unwrap();
        }
        engine.create_index("app", "docs", vec![IndexField::ascending("n")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        let filter = kimmy_query::filter::parse(&doc! { "n": { "$gte": 5, "$lte": 9 } }).unwrap();
        let plan = kimmy_query::plan::choose(&filter, &coll.indexes).expect("index applies");
        assert!(plan.both_bounds, "a scalar-only index must use both bounds");

        let candidates = engine
            .index_candidates_unless_multikey(&coll, plan.index_id, &plan.lower, &plan.upper)
            .unwrap()
            .expect("the index is not multikey");
        assert_eq!(candidates.len(), 5, "the scan must stop at the upper bound");
    }

    #[test]
    fn a_write_through_a_stale_handle_still_maintains_a_new_index() {
        // Found while writing this branch's tests: `maintain` used to take the
        // caller's index list on trust, so a write through a `CollectionMeta`
        // fetched before an index existed skipped that index entirely — no
        // entries, no unique check, no multikey observation. The definitions
        // are now re-read inside the write's own transaction.
        let (engine, stale, _dir) = engine();
        let idx = engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, None)
            .unwrap();

        // `stale` predates the index and lists none.
        assert!(stale.indexes.is_empty(), "the handle must be stale for this to prove anything");
        engine.insert(&stale, doc! { "_id": 1, "a": [1, 2] }).unwrap();

        let fresh = engine.get_collection("app", "docs").unwrap();
        assert_eq!(
            entries_for(&engine, &fresh, idx.id).len(),
            1,
            "the write must reach the index it could not see"
        );
        assert!(multikey_of(&engine, &idx.name), "and its array must be observed");
    }

    #[test]
    fn a_unique_constraint_holds_against_a_stale_handle() {
        // The sharper edge of the same hazard: a duplicate slipping through a
        // handle that predates the unique index.
        let (engine, stale, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("email")], true, None)
            .unwrap();

        engine.insert(&stale, doc! { "_id": 1, "email": "a@x.com" }).unwrap();
        let err = engine.insert(&stale, doc! { "_id": 2, "email": "a@x.com" });
        assert!(
            matches!(err, Err(StorageError::Core(CoreError::UniqueViolation { .. }))),
            "a stale handle must not bypass the constraint: {err:?}"
        );
    }

    #[test]
    fn a_checked_scan_refuses_an_index_that_went_multikey() {
        // The race this exists for: a plan built while the flag was false, and
        // a write that flipped it before the scan. The scan must say
        // "re-plan", never return candidates a too-narrow range selected.
        let (engine, _coll, _dir) = engine();
        engine.create_index("app", "docs", vec![IndexField::ascending("n")], false, None).unwrap();
        let stale = engine.get_collection("app", "docs").unwrap();

        let filter = kimmy_query::filter::parse(&doc! { "n": { "$gte": 1, "$lte": 5 } }).unwrap();
        let plan = kimmy_query::plan::choose(&filter, &stale.indexes).expect("index applies");
        assert!(plan.both_bounds);

        // The flip happens after the plan was built — exactly the window.
        engine.insert(&stale, doc! { "_id": 1i64, "n": [9, 0] }).unwrap();

        let checked = engine
            .index_candidates_unless_multikey(&stale, plan.index_id, &plan.lower, &plan.upper)
            .unwrap();
        assert_eq!(checked, None, "a flipped flag must force a re-plan, not a narrow scan");
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

    /// A two-sided range over an array field.
    ///
    /// `{a: [2, 0]}` matches `{$gte: 1, $lte: 1}` because *different elements*
    /// satisfy each bound. Intersecting both bounds into one key range excludes
    /// it — the index silently loses a matching document. Found by the
    /// equivalence proptest once it began generating two-sided ranges.
    #[test]
    fn a_two_sided_range_over_an_array_field_agrees_with_a_scan() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1i64, "a": [2, 0] }).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64, "a": [5, 5] }).unwrap();
        engine.insert(&coll, doc! { "_id": 3i64, "a": 1 }).unwrap();
        engine.create_index("app", "docs", vec![IndexField::ascending("a")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        for query in [
            doc! { "a": { "$gte": 1, "$lte": 1 } },
            doc! { "a": { "$gte": 0, "$lte": 2 } },
            doc! { "a": { "$gt": 1, "$lt": 3 } },
        ] {
            let scan = by_scan(&engine, &coll, &query);
            let indexed = by_index(&engine, &coll, &query).expect("the index should apply");
            assert_eq!(indexed, scan, "index lost documents for {query:?}");
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
                // Whether the index watches the writes arrive or backfills
                // over them. The multikey flag has one code path for each,
                // and both must license the same plans.
                index_first in any::<bool>(),
            ) {
                let (engine, coll, _dir) = engine();
                let field = if descending {
                    IndexField::descending("a")
                } else {
                    IndexField::ascending("a")
                };
                let coll = if index_first {
                    engine.create_index("app", "docs", vec![field.clone()], false, None).unwrap();
                    // Re-fetched so the writes see the index and maintain it.
                    engine.get_collection("app", "docs").unwrap()
                } else {
                    coll
                };
                for (i, v) in values.iter().enumerate() {
                    engine.insert(&coll, doc! { "_id": i as i64, "a": v.clone() }).unwrap();
                }
                if !index_first {
                    engine.create_index("app", "docs", vec![field], false, None).unwrap();
                }
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
    #[test]
    fn recreating_an_index_reuses_its_id_but_not_its_entries() {
        // Ids are derived from the name, so a recreated index necessarily gets
        // the same id. That makes purging on drop load-bearing: a surviving
        // entry would be inherited and would point at a document that no longer
        // satisfies the index.
        let (engine, _coll, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("item")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        engine.insert(&coll, doc! { "_id": 1, "item": "widget" }).unwrap();

        let first = coll.indexes[0].clone();
        engine.drop_index("app", "docs", &first.name).unwrap();

        // Recreate under the same name with the collection now empty.
        let coll = engine.get_collection("app", "docs").unwrap();
        engine.delete(&coll, &kimmy_core::DocId::Int64(1)).unwrap();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("item")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let second = &coll.indexes[0];

        assert_eq!(second.id, first.id, "a derived id is stable across drop and recreate");
        let key = kimmy_core::keyenc::encode(&bson::Bson::String("widget".into())).unwrap();
        let found = engine.index_candidates(&coll, second.id, &key, &key).unwrap();
        assert!(found.is_empty(), "the dropped index's entries must not be inherited");
    }

    #[test]
    fn two_nodes_agree_on_an_index_id_whatever_the_creation_order() {
        // The reason ids are derived: an index definition replicates, and its
        // entries are keyed by id, so the two nodes must mean the same thing.
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a = Engine::open(&a_dir.path().join("kimmy.redb")).unwrap();
        let b = Engine::open(&b_dir.path().join("kimmy.redb")).unwrap();

        for engine in [&a, &b] {
            engine.create_collection("shop", "orders").unwrap();
        }
        a.create_index("shop", "orders", vec![IndexField::ascending("email")], false, None)
            .unwrap();
        a.create_index("shop", "orders", vec![IndexField::ascending("status")], false, None)
            .unwrap();

        // Opposite order on the second node.
        b.create_index("shop", "orders", vec![IndexField::ascending("status")], false, None)
            .unwrap();
        b.create_index("shop", "orders", vec![IndexField::ascending("email")], false, None)
            .unwrap();

        let id_of = |engine: &Engine, name: &str| {
            engine
                .get_collection("shop", "orders")
                .unwrap()
                .indexes
                .iter()
                .find(|i| i.name == name)
                .unwrap()
                .id
        };
        assert_eq!(id_of(&a, "email_1"), id_of(&b, "email_1"));
        assert_eq!(id_of(&a, "status_1"), id_of(&b, "status_1"));
    }
}
