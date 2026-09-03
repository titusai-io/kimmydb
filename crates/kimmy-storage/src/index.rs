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
use kimmy_core::{CollectionId, DocId, Error as CoreError, Stamp, keyenc, path};
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
    // A partial index holds only the documents its filter selects. Returning
    // no keys here is what makes membership fall out of the ordinary
    // maintenance path: entering the filter adds entries, leaving it removes
    // them, because `apply_entries` derives both sides from this function.
    //
    // The early return also keeps a document outside the index from flipping
    // the multikey flag — an array it holds is not in the index, so it cannot
    // make an index range unsound.
    if let Some(filter) = index.partial()
        && !filter?.matches(doc)
    {
        return Ok((Vec::new(), false));
    }

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
/// [`scan_range`] inside a **write** transaction.
///
/// Separate from the read-transaction twin because redb's two transaction
/// types are distinct: `find_and_modify` matches inside the write it is about
/// to commit, which is the whole reason it is atomic.
pub(crate) fn scan_range_in_write(
    txn: &redb::WriteTransaction,
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
        self.create_index_with(db, collection, fields, unique, Enforcement::Local, name, None, None)
    }

    /// Create an index, choosing how far a unique constraint reaches and
    /// whether it expires documents.
    ///
    /// See [`Enforcement`]: `Coordinated` requires cluster machinery that does
    /// not exist yet, so it is refused rather than silently downgraded to a
    /// weaker guarantee than the caller asked for.
    ///
    /// `expire_after_secs` makes this a TTL index — see
    /// [`IndexMeta::expire_after_secs`].
    #[allow(clippy::too_many_arguments)]
    pub fn create_index_with(
        &self,
        db: &str,
        collection: &str,
        fields: Vec<IndexField>,
        unique: bool,
        enforcement: Enforcement,
        name: Option<String>,
        expire_after_secs: Option<i64>,
        partial_filter: Option<bson::Document>,
    ) -> Result<IndexMeta> {
        let (index, violations) = self.create_index_inner(
            db,
            collection,
            fields,
            unique,
            enforcement,
            name,
            expire_after_secs,
            partial_filter,
            true,
        )?;
        debug_assert!(
            violations.is_empty(),
            "a local backfill refuses a collision, never reports one"
        );
        Ok(index)
    }

    /// `log = false` when applying a replicated definition.
    ///
    /// A replicated change must not mint an entry of its own: the originating
    /// entry is appended by the caller, and minting a second one under this
    /// node's stamp would send the same change back to the peer, which would
    /// apply it and mint another. That amplifies without bound.
    ///
    /// `log` also decides what a unique index's backfill does when the
    /// existing documents already share a key, and the asymmetry is the one
    /// between [`maintain`] and [`maintain_remote`] (ADR-020). A **local**
    /// create is refused with `UniqueViolation`: the client is there to be
    /// told, and an index that reports a constraint it does not hold is
    /// worse than no index. A **replicated** create cannot be refused without
    /// abandoning convergence — the definition exists on the peer, documents
    /// written through it are on their way, and a node without the index
    /// would neither check the writes it accepts nor answer index-backed
    /// queries the way its peers do — so it is built in full, every entry
    /// added, and every colliding key comes back as a [`UniqueViolation`]
    /// naming all of its holders, for the caller to record once the build is
    /// durable the way a merged write's collisions are (ADR-029, ADR-123).
    /// The returned list is always empty on the local path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_index_inner(
        &self,
        db: &str,
        collection: &str,
        fields: Vec<IndexField>,
        unique: bool,
        enforcement: Enforcement,
        name: Option<String>,
        expire_after_secs: Option<i64>,
        partial_filter: Option<bson::Document>,
        log: bool,
    ) -> Result<(IndexMeta, Vec<UniqueViolation>)> {
        if fields.is_empty() {
            return Err(StorageError::Core(CoreError::InvalidQuery(
                "an index needs at least one field".into(),
            )));
        }
        // Parsed now and discarded: the point is to refuse an unsupported
        // shape *here*, with an operator watching, rather than at query time
        // where the only symptom would be a plan that quietly stopped
        // applying.
        if let Some(filter) = &partial_filter {
            kimmy_core::PartialFilter::parse(filter).map_err(StorageError::Core)?;
        }
        if let Some(secs) = expire_after_secs {
            if secs < 0 {
                return Err(StorageError::Core(CoreError::InvalidQuery(format!(
                    "expireAfterSeconds cannot be negative, found {secs}"
                ))));
            }
            // A compound TTL index has no meaning: expiry reads one date, and
            // there would be no rule for which field that is. Mongo refuses it
            // too, and refusing is better than silently reading the first.
            if fields.len() != 1 {
                return Err(StorageError::Core(CoreError::InvalidQuery(format!(
                    "a TTL index takes exactly one field, found {}",
                    fields.len()
                ))));
            }
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
            // The TTL is part of the definition: re-creating the same index
            // with a different `expireAfterSeconds` must not quietly return the
            // old policy and leave documents living longer than asked.
            // Each part of the definition, so the error can say which one
            // moved. "Different fields" used to be reported for all of them,
            // including a changed TTL — which sent the reader to look at the
            // one thing that had not changed.
            let mut differs = Vec::new();
            if existing.fields != fields {
                differs.push("field list");
            }
            if existing.unique != unique {
                differs.push("unique flag");
            }
            if existing.expire_after_secs != expire_after_secs {
                differs.push("expireAfterSeconds");
            }
            if existing.partial_filter != partial_filter {
                differs.push("partialFilterExpression");
            }

            if differs.is_empty() {
                return Ok((existing.clone(), Vec::new()));
            }
            return Err(StorageError::Core(CoreError::IndexExists {
                db: db.to_string(),
                collection: collection.to_string(),
                index: name,
                differs: differs.join(", "),
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

        let mut index = IndexMeta {
            id,
            name,
            fields,
            unique,
            enforcement,
            multikey: false,
            expire_after_secs,
            partial_filter,
        };

        let txn = self.begin_write()?;

        // Scoped in a closure so every table borrow ends before the abort or
        // commit below, which need to move the transaction. Returns whether the
        // existing documents already make the index multikey — the backfill is
        // the flag's only chance to see them — and, for a replicated unique
        // index, the keys the existing documents already share.
        let build = |index: &IndexMeta| -> Result<(bool, Vec<UniqueViolation>)> {
            let docs = txn.open_table(tables::DOCS)?;
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            // What a unique index has filed so far. Locally only the keys
            // matter, since the first repeat is a refusal; a replicated build
            // also keeps who holds each key, in scan order, because that is
            // what it reports — one document key per key more than the local
            // build pays, and only on the path that needs it.
            let mut seen_unique: std::collections::HashSet<Vec<u8>> = Default::default();
            let mut holders_by_key: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> =
                Default::default();
            let mut observed_multikey = false;

            for entry in docs.range(crate::engine::doc_range(meta.id))? {
                let (raw_key, raw_value) = entry?;
                let record = crate::codec::decode_doc_record(raw_value.value())?;
                let Some(doc) = record.document()? else { continue };
                let (_, doc_key) = raw_key.value();

                let (keys, multikey) = index_keys_observed(index, &doc)?;
                observed_multikey |= multikey;
                for key in keys {
                    if index.unique && log {
                        // A unique index over data that already violates it
                        // must not be created *locally* — it would report a
                        // constraint it does not actually hold.
                        if !seen_unique.insert(key.clone()) {
                            return Err(StorageError::Core(CoreError::UniqueViolation {
                                index: index.name.clone(),
                                detail: "existing documents already violate it, so it cannot \
                                         be created"
                                    .into(),
                            }));
                        }
                    } else if index.unique {
                        // Replicated, the build goes on and the collision is
                        // reported; see the doc comment on this function.
                        holders_by_key.entry(key.clone()).or_default().push(doc_key.to_vec());
                    }
                    entries.insert((meta.id.0, index.id, key.as_slice(), doc_key), ())?;
                }
            }

            // One violation per shared key, naming every holder, in the order
            // the scan met them — so the last holder is the document that
            // revealed the collision, which is what the report names as the
            // one that was merged.
            let mut violations: Vec<UniqueViolation> = holders_by_key
                .into_iter()
                .filter(|(_, holders)| holders.len() > 1)
                .map(|(key, holders)| UniqueViolation { index: index.name.clone(), key, holders })
                .collect();
            violations.sort_by(|a, b| a.key.cmp(&b.key));
            Ok((observed_multikey, violations))
        };

        let violations = match build(&index) {
            Ok((observed, violations)) => {
                index.multikey = observed;
                violations
            }
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };

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
        Ok((index, violations))
    }

    /// Drop an index and every entry it holds.
    pub fn drop_index(&self, db: &str, collection: &str, name: &str) -> Result<bool> {
        self.drop_index_inner(db, collection, name, None)
    }

    /// `replicated` is the originating stamp when applying a peer's drop, and
    /// `None` for a local one, which mints its own entry. The same parameter
    /// decides whether to log and which stamp the tombstone records, because
    /// the two must agree — the rule `drop_collection_inner` follows, for the
    /// reason ADR-034 records: a tombstone under a fresh local stamp lands
    /// ahead of a recreation that legitimately followed the drop.
    ///
    /// The tombstone (`INDEXES_DROPPED`, ADR-123) is what stops a replayed or
    /// aged-out `CreateIndex` from rebuilding the index. It is recorded even
    /// when the index is not here to drop, on the replicated path: a node
    /// that never held the index still needs to know the definition is
    /// history, or a later replay of the create builds it. A *local* drop of
    /// an index that is not there records nothing — it mints no entry, so a
    /// tombstone would be a decision this node's peers never hear of, and it
    /// would make this node refuse a definition every other member accepts.
    pub(crate) fn drop_index_inner(
        &self,
        db: &str,
        collection: &str,
        name: &str,
        replicated: Option<Stamp>,
    ) -> Result<bool> {
        let mut meta = self.get_collection(db, collection)?;
        // Derived from the name rather than read from the definition, so the
        // key agrees with what a `CreateIndex` replay will compute whether or
        // not the index is here.
        let index_id = IndexMeta::derive_id(name);
        let Some(index) = meta.index(name).cloned() else {
            if let Some(stamp) = replicated {
                self.record_index_drop(meta.id, index_id, stamp)?;
            }
            return Ok(false);
        };

        let stamp = replicated.unwrap_or_else(|| self.next_stamp());
        let log = replicated.is_none();
        let txn = self.begin_write()?;
        {
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            entries.retain_in(index_id_range(meta.id, index.id), |_, _| false)?;
        }
        // Entries are removed above, in this same transaction, which is what
        // makes it safe for an index later recreated under the same name to
        // receive the same derived id — it cannot inherit anything.
        meta.indexes.retain(|i| i.name != name);
        crate::Engine::put_collection_meta(&txn, &meta)?;
        // Same transaction as the removal, so there is no instant in which the
        // index is gone with no record that it was dropped.
        crate::Engine::record_index_drop_in_txn(&txn, meta.id, index_id, stamp)?;

        let logged = if log {
            let entry = crate::engine::ddl_entry(
                stamp,
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
        Ok(self.get_record_by_encoded_key(coll, key)?.map(|(_, doc)| doc))
    }

    /// [`Engine::get_by_encoded_key`], with the document's stamp.
    pub fn get_record_by_encoded_key(
        &self,
        coll: &crate::CollectionMeta,
        key: &[u8],
    ) -> Result<Option<(kimmy_core::Stamp, Document)>> {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        match docs.get((coll.id.0, key))? {
            Some(raw) => {
                let record = crate::codec::decode_doc_record(raw.value())?;
                Ok(record.document()?.map(|doc| (record.stamp, doc)))
            }
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming candidates
// ---------------------------------------------------------------------------

/// One index scan, as the planner describes it.
///
/// Encoded byte ranges rather than a query type, as [`crate::Candidates`]
/// does, so the crate boundary stays where it is.
#[derive(Clone, Copy, Debug)]
pub struct IndexScan<'a> {
    pub index_id: u32,
    /// Inclusive `(lower, upper)` key ranges, in key order and disjoint: one
    /// for a plain plan, one per probe for a `$in` union.
    pub ranges: &'a [(Vec<u8>, Vec<u8>)],
    /// Whether the ranges intersect both ends of a range — sound only while
    /// the index is not multikey, so the flag is re-read in the scanning
    /// snapshot and the scan refused if it has flipped.
    pub both_bounds: bool,
    /// Whether every range pins one complete index key, so its entries are
    /// already in document-key order and hold each document once. What the
    /// planner reports as `IndexPlan::exact`.
    pub exact: bool,
}

/// The order an index scan delivers its candidates in.
#[derive(Clone, Copy, Debug)]
pub enum CandidateOrder<'a> {
    /// Index order — whatever order the entries are stored in. The cheapest
    /// delivery: one pass, nothing held back. For a caller that does not care
    /// about order, which is a `count` or a query that sorts afterwards.
    Any,
    /// Ascending document key, strictly after `after`. The order a cursor
    /// pages in.
    ///
    /// `want` is how many candidates the caller expects to accept before it
    /// stops. It sizes the work an inexact range does per pass and bounds
    /// what the scan holds; `None` means every candidate, in order.
    ById { after: Option<&'a [u8]>, want: Option<usize> },
}

/// What an index scan did.
///
/// `entries` is the honest measure of how much of the index a query touched,
/// as distinct from how many documents it went on to examine: an exact probe
/// stopped after one match reads one entry however many the key holds, and a
/// range that had to be put in `_id` order reads all of its entries however
/// few documents it returns.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexScanOutcome {
    /// Index entries read.
    pub entries: usize,
    /// Passes over the ranges. One, except for an inexact range delivered in
    /// `_id` order whose first pass did not yield enough documents past the
    /// recheck, which goes back for more.
    pub passes: usize,
}

/// The tables one scan reads from, and the visitor it feeds.
///
/// Documents are fetched from the **same snapshot** as the index entries —
/// one read transaction per query — where the older candidate list opened a
/// new transaction per document.
struct Walk<'t, F> {
    coll: CollectionId,
    index: &'t IndexMeta,
    entries: &'t redb::ReadOnlyTable<tables::IndexKey<'static>, ()>,
    docs: &'t redb::ReadOnlyTable<(u64, &'static [u8]), &'static [u8]>,
    ranges: &'t [(Vec<u8>, Vec<u8>)],
    outcome: IndexScanOutcome,
    visit: F,
}

impl<F> Walk<'_, F>
where
    F: FnMut(&[u8], kimmy_core::Stamp, Document) -> Result<bool>,
{
    /// The live document under a key, or `None` for a tombstone or a gap.
    ///
    /// A gap is an ordinary miss, not an error: index entries are removed
    /// lazily after a drop, and a candidate whose document is gone is simply
    /// not a candidate.
    fn load(&self, key: &[u8]) -> Result<Option<(kimmy_core::Stamp, Document)>> {
        match self.docs.get((self.coll.0, key))? {
            Some(raw) => {
                let record = crate::codec::decode_doc_record(raw.value())?;
                Ok(record.document()?.map(|doc| (record.stamp, doc)))
            }
            None => Ok(None),
        }
    }

    /// Hand one candidate to the visitor. `Ok(false)` means stop.
    fn offer(&mut self, key: &[u8]) -> Result<bool> {
        match self.load(key)? {
            Some((stamp, doc)) => (self.visit)(key, stamp, doc),
            None => Ok(true),
        }
    }

    /// Whether `key` is the first entry in these ranges that names `doc`.
    ///
    /// A multikey index holds a document under every key its arrays
    /// contribute, so a range that covers two of them meets the document
    /// twice. Rather than remember every document seen — a set that grows
    /// with the range — this recomputes the document's keys and accepts it
    /// only at the smallest one the scan covers. The scan visits the ranges
    /// in key order, so that is exactly the entry it met first.
    fn first_entry_for(&self, doc: &Document, key: &[u8]) -> Result<bool> {
        // `index_keys` returns them sorted, so the first in range is the least.
        let keys = index_keys(self.index, doc)?;
        let first = keys.iter().find(|k| {
            self.ranges.iter().any(|(lower, upper)| {
                k.as_slice() >= lower.as_slice() && k.as_slice() <= upper.as_slice()
            })
        });
        Ok(first.is_some_and(|k| k.as_slice() == key))
    }

    /// Every entry of every range, in index order.
    fn in_index_order(&mut self) -> Result<()> {
        use std::ops::Bound;
        self.outcome.passes = 1;
        let multikey = self.index.multikey;
        for (lower, upper) in self.ranges {
            let start =
                Bound::Included((self.coll.0, self.index.id, lower.as_slice(), [].as_slice()));
            for entry in self.entries.range::<tables::IndexKey<'_>>((start, Bound::Unbounded))? {
                let (found, _) = entry?;
                let (c, i, k, doc_key) = found.value();
                if c != self.coll.0 || i != self.index.id || k > upper.as_slice() {
                    break;
                }
                self.outcome.entries += 1;
                let Some((stamp, doc)) = self.load(doc_key)? else {
                    continue;
                };
                if multikey && !self.first_entry_for(&doc, k)? {
                    continue;
                }
                if !(self.visit)(doc_key, stamp, doc)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// One exact probe: a single run of entries under one key, already in
    /// document-key order and holding each document once. The bound is a
    /// seek, so resuming costs nothing, and the visitor stopping stops the
    /// read — a `limit: 1` reads one entry.
    fn one_run(&mut self, after: Option<&[u8]>) -> Result<()> {
        use std::ops::Bound;
        self.outcome.passes = 1;
        let (lower, upper) = &self.ranges[0];
        let start = match after {
            Some(bound) => Bound::Excluded((self.coll.0, self.index.id, lower.as_slice(), bound)),
            None => Bound::Included((self.coll.0, self.index.id, lower.as_slice(), [].as_slice())),
        };
        for entry in self.entries.range::<tables::IndexKey<'_>>((start, Bound::Unbounded))? {
            let (found, _) = entry?;
            let (c, i, k, doc_key) = found.value();
            if c != self.coll.0 || i != self.index.id || k > upper.as_slice() {
                break;
            }
            self.outcome.entries += 1;
            if !self.offer(doc_key)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Several exact probes: one sorted run each, merged by their heads.
    ///
    /// A `$in` used to gather every probe's candidates into one set and walk
    /// it, which held the union of the probes whatever the caller wanted
    /// from it. The merge holds one head per probe. A document under two
    /// probes — an array holding two of the listed values — surfaces as the
    /// same key twice in a row, and the second is dropped.
    fn merged_runs(&mut self, after: Option<&[u8]>) -> Result<()> {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        use std::ops::Bound;

        self.outcome.passes = 1;
        let (coll, index_id) = (self.coll.0, self.index.id);
        let mut runs = Vec::with_capacity(self.ranges.len());
        for (lower, _) in self.ranges {
            let start = match after {
                Some(bound) => Bound::Excluded((coll, index_id, lower.as_slice(), bound)),
                None => Bound::Included((coll, index_id, lower.as_slice(), [].as_slice())),
            };
            runs.push(self.entries.range::<tables::IndexKey<'_>>((start, Bound::Unbounded))?);
        }

        // The next document key of run `i`, or `None` once it has left its
        // probe's range. Counted here, since this is where entries are read.
        let ranges = self.ranges;
        let mut next = |i: usize, entries: &mut usize| -> Result<Option<Vec<u8>>> {
            let upper = ranges[i].1.as_slice();
            match runs[i].next() {
                Some(entry) => {
                    let (found, _) = entry?;
                    let (c, ix, k, doc_key) = found.value();
                    if c != coll || ix != index_id || k > upper {
                        return Ok(None);
                    }
                    *entries += 1;
                    Ok(Some(doc_key.to_vec()))
                }
                None => Ok(None),
            }
        };

        let mut heads = BinaryHeap::new();
        for i in 0..ranges.len() {
            if let Some(key) = next(i, &mut self.outcome.entries)? {
                heads.push(Reverse((key, i)));
            }
        }

        let mut last: Option<Vec<u8>> = None;
        while let Some(Reverse((key, i))) = heads.pop() {
            if let Some(following) = next(i, &mut self.outcome.entries)? {
                heads.push(Reverse((following, i)));
            }
            if last.as_deref() == Some(key.as_slice()) {
                continue;
            }
            if !self.offer(&key)? {
                return Ok(());
            }
            last = Some(key);
        }
        Ok(())
    }

    /// An inexact range, in document-key order, holding at most `want` keys.
    ///
    /// Entries under a range of keys are ordered by key first, so their
    /// document keys interleave and no seek finds "the next `_id`". The scan
    /// used to collect the whole range and sort it. This reads the range and
    /// keeps only the `want` smallest document keys past `after` — a bounded
    /// set, the work of one pass, and the same keys the sort would have put
    /// first. They are offered in order; if the recheck rejects enough of
    /// them that the visitor is still going when they run out, the next pass
    /// resumes after the last key seen and asks for twice as many, so the
    /// number of passes is logarithmic in what the filter rejected rather
    /// than linear in it.
    fn in_key_order(&mut self, after: Option<&[u8]>, want: Option<usize>) -> Result<()> {
        use std::collections::BTreeSet;
        use std::ops::Bound;

        let mut after: Option<Vec<u8>> = after.map(<[u8]>::to_vec);
        let mut batch = want.unwrap_or(usize::MAX).max(1);
        loop {
            self.outcome.passes += 1;
            let mut best: BTreeSet<Vec<u8>> = BTreeSet::new();
            for (lower, upper) in self.ranges {
                let start =
                    Bound::Included((self.coll.0, self.index.id, lower.as_slice(), [].as_slice()));
                for entry in
                    self.entries.range::<tables::IndexKey<'_>>((start, Bound::Unbounded))?
                {
                    let (found, _) = entry?;
                    let (c, i, k, doc_key) = found.value();
                    if c != self.coll.0 || i != self.index.id || k > upper.as_slice() {
                        break;
                    }
                    self.outcome.entries += 1;
                    if after.as_deref().is_some_and(|bound| doc_key <= bound) {
                        continue;
                    }
                    if best.len() < batch {
                        best.insert(doc_key.to_vec());
                    } else if best.last().is_some_and(|max| doc_key < max.as_slice())
                        && best.insert(doc_key.to_vec())
                    {
                        // Grew past the bound with a smaller key: the largest
                        // is no longer among the `batch` smallest. (A repeat
                        // of a key already held does not grow the set.)
                        best.pop_last();
                    }
                }
            }
            // Fewer than asked for means the ranges hold nothing more past
            // `after`: this pass is the last whatever the visitor says.
            let exhausted = best.len() < batch;
            let mut last = None;
            for key in best {
                if !self.offer(&key)? {
                    return Ok(());
                }
                last = Some(key);
            }
            if exhausted {
                return Ok(());
            }
            after = last;
            batch = batch.saturating_mul(2);
        }
    }
}

impl crate::Engine {
    /// Stream the documents an index scan points at, rechecked by the caller.
    ///
    /// These are **candidates**: an index says which documents *might*
    /// match, and the visitor must apply the full filter before counting one.
    /// The visitor returns whether to continue, and stopping stops the read —
    /// what makes a `limit` bound the work rather than only the result.
    ///
    /// Nothing proportional to the range is held. An exact probe is one run
    /// read in place; a union is a merge holding one head per probe; an
    /// inexact range in `_id` order holds at most `want` keys per pass. See
    /// [`CandidateOrder`] for what each delivery promises.
    ///
    /// `None` means the scan was refused and the caller must re-plan — the
    /// same rule as [`Engine::index_candidates_unless_multikey`]: a plan that
    /// intersected both bounds is only sound while the index is not multikey,
    /// and the flag is re-read here in the same snapshot as the entries. An
    /// index that no longer exists is refused too; a collection that no
    /// longer exists yields nothing, which in this snapshot is the truth.
    pub fn visit_index_candidates<F>(
        &self,
        coll: &crate::CollectionMeta,
        scan: &IndexScan<'_>,
        order: CandidateOrder<'_>,
        visit: F,
    ) -> Result<Option<IndexScanOutcome>>
    where
        F: FnMut(&[u8], kimmy_core::Stamp, Document) -> Result<bool>,
    {
        let txn = self.db().begin_read()?;
        let index = {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            let fresh: crate::CollectionMeta =
                match collections.get((coll.db.as_str(), coll.name.as_str()))? {
                    Some(raw) => serde_json::from_slice(raw.value())?,
                    None => return Ok(Some(IndexScanOutcome::default())),
                };
            match fresh.index_by_id(scan.index_id) {
                Some(index) if !(scan.both_bounds && index.multikey) => index.clone(),
                _ => return Ok(None),
            }
        };
        let entries = txn.open_table(tables::INDEX_ENTRIES)?;
        let docs = txn.open_table(tables::DOCS)?;
        let mut walk = Walk {
            coll: coll.id,
            index: &index,
            entries: &entries,
            docs: &docs,
            ranges: scan.ranges,
            outcome: IndexScanOutcome::default(),
            visit,
        };
        if !scan.ranges.is_empty() {
            match order {
                CandidateOrder::Any => walk.in_index_order()?,
                CandidateOrder::ById { after, .. } if scan.exact && scan.ranges.len() == 1 => {
                    walk.one_run(after)?;
                }
                CandidateOrder::ById { after, .. } if scan.exact => walk.merged_runs(after)?,
                CandidateOrder::ById { after, want } => walk.in_key_order(after, want)?,
            }
        }
        Ok(Some(walk.outcome))
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
            expire_after_secs: None,
            partial_filter: None,
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
    fn a_conflicting_index_says_it_is_an_index_and_names_what_moved() {
        // The message used to borrow `CollectionExists`, which produced a
        // sentence built for a collection wrapped around an index:
        //
        //   collection "app"."docs.i (index already exists with different
        //   fields)" already exists
        //
        // Nothing in it is quite true. The thing that exists is an index, not
        // a collection called that, and the caller is left to guess what to do.
        let (engine, _coll, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, Some("i".into()))
            .unwrap();

        let err = engine
            .create_index("app", "docs", vec![IndexField::ascending("b")], false, Some("i".into()))
            .unwrap_err()
            .to_string();

        assert!(err.contains("index \"i\""), "must say an index is what exists: {err}");
        assert!(err.contains("field list"), "must name what differs: {err}");
        assert!(!err.contains("collection \"app\".\"docs.i"), "no collection-shaped name: {err}");
        assert!(err.contains("another name"), "must offer a way out: {err}");
    }

    #[test]
    fn a_conflict_over_the_ttl_does_not_blame_the_fields() {
        // The inaccuracy that mattered most: every conflict reported
        // "different fields", including one where the fields were identical
        // and only the expiry had moved — sending the reader to check the one
        // part of the definition that had not changed.
        let (engine, _coll, _dir) = engine();
        let fields = vec![IndexField::ascending("seen")];
        engine
            .create_index_with(
                "app",
                "docs",
                fields.clone(),
                false,
                Enforcement::Local,
                Some("ttl".into()),
                Some(60),
                None,
            )
            .unwrap();

        let err = engine
            .create_index_with(
                "app",
                "docs",
                fields,
                false,
                Enforcement::Local,
                Some("ttl".into()),
                Some(120),
                None,
            )
            .unwrap_err()
            .to_string();

        assert!(err.contains("expireAfterSeconds"), "must name the TTL: {err}");
        assert!(!err.contains("field list"), "the fields did not change: {err}");
    }

    #[test]
    fn a_conflict_over_several_parts_names_all_of_them() {
        let (engine, _coll, _dir) = engine();
        engine
            .create_index("app", "docs", vec![IndexField::ascending("a")], false, Some("i".into()))
            .unwrap();

        let err = engine
            .create_index("app", "docs", vec![IndexField::ascending("b")], true, Some("i".into()))
            .unwrap_err()
            .to_string();

        assert!(err.contains("field list"), "{err}");
        assert!(err.contains("unique flag"), "{err}");
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
            None,
            None,
        );
        assert!(matches!(err, Err(StorageError::Core(CoreError::Unsupported(_)))));
    }

    #[test]
    fn a_ttl_index_must_be_single_field() {
        // A compound TTL index has no meaning: expiry reads one date and there
        // is no rule for which field that is. Refusing beats reading the first.
        let (engine, _coll, _dir) = engine();
        let err = engine.create_index_with(
            "app",
            "docs",
            vec![IndexField::ascending("a"), IndexField::ascending("b")],
            false,
            Enforcement::Local,
            Some("ttl_ab".into()),
            Some(60),
            None,
        );
        assert!(matches!(err, Err(StorageError::Core(CoreError::InvalidQuery(_)))), "{err:?}");
    }

    #[test]
    fn a_negative_expiry_is_refused() {
        let (engine, _coll, _dir) = engine();
        let err = engine.create_index_with(
            "app",
            "docs",
            vec![IndexField::ascending("seen")],
            false,
            Enforcement::Local,
            Some("ttl_seen".into()),
            Some(-1),
            None,
        );
        assert!(matches!(err, Err(StorageError::Core(CoreError::InvalidQuery(_)))), "{err:?}");
    }

    #[test]
    fn zero_seconds_is_a_valid_policy() {
        // "expire as soon as the date passes" is a real thing to want, and is
        // how Mongo's `expireAfterSeconds: 0` absolute-deadline pattern works.
        let (engine, _coll, _dir) = engine();
        let index = engine
            .create_index_with(
                "app",
                "docs",
                vec![IndexField::ascending("expiresAt")],
                false,
                Enforcement::Local,
                Some("ttl_at".into()),
                Some(0),
                None,
            )
            .unwrap();
        assert!(index.is_ttl(), "zero must not be read as absent");
        assert_eq!(index.ttl_path(), Some("expiresAt"));
    }

    #[test]
    fn recreating_with_a_different_expiry_is_a_conflict_not_a_silent_keep() {
        // Returning the old definition here would leave documents living
        // longer than the caller just asked for, with a 200 saying otherwise.
        let (engine, _coll, _dir) = engine();
        let fields = vec![IndexField::ascending("seen")];
        engine
            .create_index_with(
                "app",
                "docs",
                fields.clone(),
                false,
                Enforcement::Local,
                Some("ttl_seen".into()),
                Some(60),
                None,
            )
            .unwrap();

        // Same policy: idempotent.
        assert!(
            engine
                .create_index_with(
                    "app",
                    "docs",
                    fields.clone(),
                    false,
                    Enforcement::Local,
                    Some("ttl_seen".into()),
                    Some(60),
                    None,
                )
                .is_ok()
        );

        // Different policy: refused.
        let err = engine.create_index_with(
            "app",
            "docs",
            fields,
            false,
            Enforcement::Local,
            Some("ttl_seen".into()),
            Some(30),
            None,
        );
        assert!(err.is_err(), "a changed TTL must not be silently ignored");
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
    fn a_dollar_in_reads_only_its_probes() {
        // The selectivity `$in` planning buys: twenty documents, two listed
        // values, two candidates — where the collection scan it used to fall
        // back to reads all twenty.
        let (engine, coll, _dir) = engine();
        for i in 0..20 {
            engine.insert(&coll, doc! { "_id": i as i64, "n": i }).unwrap();
        }
        engine.create_index("app", "docs", vec![IndexField::ascending("n")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        let filter = kimmy_query::filter::parse(&doc! { "n": { "$in": [3, 17] } }).unwrap();
        let plan = kimmy_query::plan::choose(&filter, &coll.indexes).expect("$in should plan");
        assert_eq!(plan.ranges.len(), 2);

        let mut candidates = std::collections::BTreeSet::new();
        for (lower, upper) in &plan.ranges {
            candidates.extend(engine.index_candidates(&coll, plan.index_id, lower, upper).unwrap());
        }
        assert_eq!(candidates.len(), 2, "two probes, two candidates, eighteen never touched");
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
            .index_candidates_unless_multikey(
                &coll,
                plan.index_id,
                &plan.ranges[0].0,
                &plan.ranges[0].1,
            )
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
            .index_candidates_unless_multikey(
                &stale,
                plan.index_id,
                &plan.ranges[0].0,
                &plan.ranges[0].1,
            )
            .unwrap();
        assert_eq!(checked, None, "a flipped flag must force a re-plan, not a narrow scan");
    }

    // -----------------------------------------------------------------------
    // Streaming candidates — bounded by what the caller takes, not the range
    // -----------------------------------------------------------------------

    /// The plan `exec` would run for a filter, with the planner's own flags.
    fn plan_for(coll: &CollectionMeta, query: &Document) -> kimmy_query::plan::IndexPlan {
        let filter = kimmy_query::filter::parse(query).unwrap();
        kimmy_query::plan::choose(&filter, &coll.indexes).expect("the index should apply")
    }

    /// Walk a plan in the given order, taking every candidate until `stop`
    /// says otherwise; the ids in arrival order, plus what the scan did.
    fn walk(
        engine: &Engine,
        coll: &CollectionMeta,
        plan: &kimmy_query::plan::IndexPlan,
        order: CandidateOrder<'_>,
        mut accept: impl FnMut(i64) -> bool,
        stop_after: usize,
    ) -> (Vec<i64>, IndexScanOutcome) {
        let scan = IndexScan {
            index_id: plan.index_id,
            ranges: &plan.ranges,
            both_bounds: plan.both_bounds,
            exact: plan.exact,
        };
        let mut ids = Vec::new();
        let outcome = engine
            .visit_index_candidates(coll, &scan, order, |_, _, doc| {
                let id = doc.get_i64("_id").unwrap();
                if accept(id) {
                    ids.push(id);
                }
                Ok(ids.len() < stop_after)
            })
            .unwrap()
            .expect("the scan should not be refused");
        (ids, outcome)
    }

    #[test]
    fn an_exact_probe_stopped_after_one_reads_one_entry() {
        // The shape that used to cost the most for the least: an unselective
        // equality with `limit: 1` gathered every candidate key under the
        // value, sorted them, and read the first. Now the first entry is the
        // first document and the read stops there.
        let (engine, coll, _dir) = engine();
        for i in 0..200i64 {
            engine.insert(&coll, doc! { "_id": i, "k": 1 }).unwrap();
        }
        engine.create_index("app", "docs", vec![IndexField::ascending("k")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let plan = plan_for(&coll, &doc! { "k": 1 });
        assert!(plan.exact);

        let by_id = CandidateOrder::ById { after: None, want: Some(1) };
        let (ids, outcome) = walk(&engine, &coll, &plan, by_id, |_| true, 1);
        assert_eq!(ids, vec![0], "the least _id, not an arbitrary one");
        assert_eq!(outcome.entries, 1, "one wanted, one read");
        assert_eq!(outcome.passes, 1);

        // Resuming is a seek, not a skip: the entries before the bound are
        // never read.
        let bound = doc_key_for(&DocId::Int64(150)).unwrap();
        let resumed = CandidateOrder::ById { after: Some(&bound), want: Some(2) };
        let (ids, outcome) = walk(&engine, &coll, &plan, resumed, |_| true, 2);
        assert_eq!(ids, vec![151, 152]);
        assert_eq!(outcome.entries, 2);

        // Taking everything reads everything, once.
        let all = CandidateOrder::ById { after: None, want: None };
        let (ids, outcome) = walk(&engine, &coll, &plan, all, |_| true, usize::MAX);
        assert_eq!(ids, (0..200).collect::<Vec<_>>());
        assert_eq!(outcome.entries, 200);
    }

    #[test]
    fn a_union_of_probes_names_each_document_once_in_id_order() {
        // An array holding two of the listed values files the document under
        // two probes. The merge must surface it once, and in `_id` order
        // across the probes — the order a cursor resumes in.
        let (engine, coll, _dir) = engine();
        for (id, tags) in [
            (1i64, vec!["a", "b"]),
            (2, vec!["b"]),
            (3, vec!["a"]),
            (4, vec!["c"]),
            (5, vec!["b", "a"]),
            (6, vec!["a", "c"]),
        ] {
            engine.insert(&coll, doc! { "_id": id, "tags": tags }).unwrap();
        }
        engine
            .create_index("app", "docs", vec![IndexField::ascending("tags")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let plan = plan_for(&coll, &doc! { "tags": { "$in": ["b", "a"] } });
        assert_eq!(plan.ranges.len(), 2);
        assert!(plan.exact);

        let by_id = CandidateOrder::ById { after: None, want: None };
        let (ids, _) = walk(&engine, &coll, &plan, by_id, |_| true, usize::MAX);
        assert_eq!(ids, vec![1, 2, 3, 5, 6], "once each, ascending, across both probes");

        // Index order has no order to promise, but it must still name each
        // document once — here by recomputing its keys rather than by
        // remembering every document seen.
        let (mut any, _) = walk(&engine, &coll, &plan, CandidateOrder::Any, |_| true, usize::MAX);
        any.sort_unstable();
        assert_eq!(any, vec![1, 2, 3, 5, 6]);

        // A page resumed from the middle picks up exactly where it stopped.
        let bound = doc_key_for(&DocId::Int64(2)).unwrap();
        let resumed = CandidateOrder::ById { after: Some(&bound), want: Some(2) };
        let (ids, _) = walk(&engine, &coll, &plan, resumed, |_| true, 2);
        assert_eq!(ids, vec![3, 5]);
    }

    #[test]
    fn an_inexact_range_arrives_in_id_order_and_holds_only_the_window() {
        // Entries under a range of keys are in key order, so their document
        // keys interleave: `_id` order has to be recovered. The old scan
        // sorted the whole range; this keeps the `want` smallest per pass.
        let (engine, coll, _dir) = engine();
        for i in 0..60i64 {
            // Chosen so the index order is nothing like the `_id` order.
            engine.insert(&coll, doc! { "_id": i, "n": (i * 37) % 60 }).unwrap();
        }
        engine.create_index("app", "docs", vec![IndexField::ascending("n")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let query = doc! { "n": { "$gte": 20 } };
        let plan = plan_for(&coll, &query);
        assert!(!plan.exact);
        let expected = by_scan(&engine, &coll, &query);
        assert_eq!(expected.len(), 40);

        // Everything wanted, in order, in one pass.
        let all = CandidateOrder::ById { after: None, want: None };
        let (ids, outcome) = walk(&engine, &coll, &plan, all, |_| true, usize::MAX);
        assert_eq!(ids, expected);
        assert_eq!(outcome.passes, 1);
        assert_eq!(outcome.entries, 40, "one pass reads the range once");

        // Three wanted: the three least, in one pass — the range is read but
        // only three keys are ever held.
        let three = CandidateOrder::ById { after: None, want: Some(3) };
        let (ids, outcome) = walk(&engine, &coll, &plan, three, |_| true, 3);
        assert_eq!(ids, expected[..3]);
        assert_eq!(outcome.passes, 1);

        // A recheck that rejects most of what the first pass offers: the
        // scan goes back for more, doubling, and the order still holds.
        let picky = CandidateOrder::ById { after: None, want: Some(2) };
        let (ids, outcome) = walk(&engine, &coll, &plan, picky, |id| id % 10 == 0, usize::MAX);
        let wanted: Vec<i64> = expected.iter().copied().filter(|id| id % 10 == 0).collect();
        assert_eq!(ids, wanted);
        assert!(outcome.passes > 1, "the first pass of two cannot have satisfied {wanted:?}");

        // Resuming after a key delivers strictly what follows it.
        let bound = doc_key_for(&DocId::Int64(expected[10])).unwrap();
        let resumed = CandidateOrder::ById { after: Some(&bound), want: Some(5) };
        let (ids, _) = walk(&engine, &coll, &plan, resumed, |_| true, 5);
        assert_eq!(ids, expected[11..16]);
    }

    #[test]
    fn a_multikey_range_in_index_order_names_each_document_once() {
        // A range covering two of one document's array values meets the
        // document at both. Index order carries no dedup set; the document
        // is taken at the first of its keys the range covers and skipped at
        // the rest.
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1i64, "n": [3, 7, 9] }).unwrap();
        engine.insert(&coll, doc! { "_id": 2i64, "n": 5 }).unwrap();
        engine.insert(&coll, doc! { "_id": 3i64, "n": [1, 8] }).unwrap();
        engine.create_index("app", "docs", vec![IndexField::ascending("n")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        assert!(coll.indexes[0].multikey);

        let query = doc! { "n": { "$gte": 4 } };
        let plan = plan_for(&coll, &query);
        let (mut any, outcome) =
            walk(&engine, &coll, &plan, CandidateOrder::Any, |_| true, usize::MAX);
        any.sort_unstable();
        assert_eq!(any, vec![1, 2, 3]);
        // Each element is an entry, and so is the whole array — which, being an
        // array, sorts above every number and so falls inside an open-topped
        // range too. Six entries name three documents.
        assert_eq!(outcome.entries, 6, "7, 9, [3,7,9] for _id 1; 5 for _id 2; 8, [1,8] for _id 3");

        // And the ordered delivery agrees, with the range's keys interleaved.
        let by_id = CandidateOrder::ById { after: None, want: None };
        let (ids, _) = walk(&engine, &coll, &plan, by_id, |_| true, usize::MAX);
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn a_streamed_scan_refuses_an_index_that_went_multikey() {
        // The same rule the candidate list follows, on the path `find` now
        // takes: a both-bounds plan is refused — before the visitor sees
        // anything — once the index is multikey in the scanning snapshot.
        let (engine, _coll, _dir) = engine();
        engine.create_index("app", "docs", vec![IndexField::ascending("n")], false, None).unwrap();
        let stale = engine.get_collection("app", "docs").unwrap();
        let plan = plan_for(&stale, &doc! { "n": { "$gte": 1, "$lte": 5 } });
        assert!(plan.both_bounds);

        engine.insert(&stale, doc! { "_id": 1i64, "n": [9, 0] }).unwrap();

        let scan = IndexScan {
            index_id: plan.index_id,
            ranges: &plan.ranges,
            both_bounds: true,
            exact: plan.exact,
        };
        let mut visited = 0;
        let refused = engine
            .visit_index_candidates(&stale, &scan, CandidateOrder::Any, |_, _, _| {
                visited += 1;
                Ok(true)
            })
            .unwrap();
        assert_eq!(refused, None, "a flipped flag must force a re-plan");
        assert_eq!(visited, 0, "and nothing may have been handed out first");
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

        // The union over the plan's ranges, exactly as `exec` performs it —
        // one range for a plain plan, several for a `$in`.
        let mut candidates = std::collections::BTreeSet::new();
        for (lower, upper) in &plan.ranges {
            candidates.extend(engine.index_candidates(coll, plan.index_id, lower, upper).unwrap());
        }

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
            // $in: unions of point probes, over scalars, over a multikey
            // index (a document whose array holds two listed values must
            // appear once), with duplicate spellings, and after a prefix.
            doc! { "a": { "$in": [1, 2] } },
            doc! { "a": { "$in": [1, 1.0] } },
            doc! { "a": { "$in": [999] } },
            doc! { "a": { "$in": [] } },
            doc! { "tags": { "$in": ["x", "y"] } },
            doc! { "n": { "$in": [10, 40] } },
            doc! { "a": 1, "n": { "$in": [10, 20] } },
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
            exercised >= 18,
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
            // `expect` rather than `if let`: descending ranges are planned
            // now, so a missing plan is a regression, not a skip. This test
            // once passed vacuously for exactly that reason.
            let indexed =
                by_index(&engine, &coll, &query).expect("the descending index should be used");
            assert_eq!(indexed, scan, "descending index disagreed for {query:?}");
        }
    }

    #[test]
    fn a_two_sided_range_on_a_descending_scalar_index_reads_only_the_range() {
        // The same selectivity proof as the ascending one: twenty scalars, a
        // range covering five, five candidates. A swap done backwards shows
        // up here as zero candidates — an empty range, silently wrong — and
        // the equivalence tests above would catch the lost documents.
        let (engine, coll, _dir) = engine();
        for i in 0..20 {
            engine.insert(&coll, doc! { "_id": i as i64, "n": i }).unwrap();
        }
        engine.create_index("app", "docs", vec![IndexField::descending("n")], false, None).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        let filter = kimmy_query::filter::parse(&doc! { "n": { "$gte": 5, "$lte": 9 } }).unwrap();
        let plan = kimmy_query::plan::choose(&filter, &coll.indexes).expect("index applies");
        assert!(plan.both_bounds);

        let candidates = engine
            .index_candidates_unless_multikey(
                &coll,
                plan.index_id,
                &plan.ranges[0].0,
                &plan.ranges[0].1,
            )
            .unwrap()
            .expect("not multikey");
        assert_eq!(candidates.len(), 5, "the scan must stop at both ends of the range");
    }

    #[test]
    fn a_descending_range_after_an_equality_prefix_agrees_with_a_scan() {
        // The compound shape the old fallback dropped entirely. The prefix is
        // ascending, the range field descending, and the range must narrow
        // within the prefix rather than fall back to scanning all of a == 1.
        let (engine, coll, _dir) = engine();
        for i in 0..10 {
            engine.insert(&coll, doc! { "_id": i as i64, "a": i % 2, "n": i }).unwrap();
        }
        engine
            .create_index(
                "app",
                "docs",
                vec![IndexField::ascending("a"), IndexField::descending("n")],
                false,
                None,
            )
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        for query in [
            doc! { "a": 1, "n": { "$gt": 2 } },
            doc! { "a": 0, "n": { "$gte": 2, "$lte": 6 } },
            doc! { "a": 1, "n": { "$lt": 7 } },
        ] {
            let scan = by_scan(&engine, &coll, &query);
            assert!(!scan.is_empty(), "{query:?} should match something");
            let indexed = by_index(&engine, &coll, &query).expect("the index should be used");
            assert_eq!(indexed, scan, "compound descending disagreed for {query:?}");
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
                shape in 0u8..4,
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
                    // A union of point probes, including over arrays — a
                    // document whose array holds both listed values must
                    // appear once, not twice.
                    2 => doc! { "a": { "$in": [probe, probe + span] } },
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
