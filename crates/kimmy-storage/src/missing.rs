//! The missing-through vector of a `field` document (the field rewind, design q7-step3-rewind-design.md, 12.1 option (a)).
//!
//! The rewind of a document is the fold of the entries it still holds at or below the target. An entry retention
//! collected, or one a snapshot page never sent (the page gave the document as state), is not in the fold, and
//! the rewind must tell "that writer's write to this document was in the missing range" from "that writer wrote
//! something else then": refusing every document a writer has ever touched whose origin has collected anything
//! would refuse the common case.
//!
//! So each document keeps, per writer (origin), the newest stamp of that writer's write to it that this member
//! no longer holds as an entry: raised in the transaction that removes the entry (retention), and, for a state
//! installed as state (a page document, a carried delete), to the version vector of the installed state, since
//! every write it has seen is held only as that state. A rewind refuses a document only if some writer's missing
//! stamp lies above what the fold of the retained entries saw of that writer and at or below the target.
//!
//! **Node-local and outside the replicated state**: not in `meta`, not in the state hash. Carried by a backup
//! (the oplog is, and a restored member must not forget what it had collected). A row goes with the document
//! when retention collects its tombstone and with its collection when the drop purger removes it.

use kimmy_core::{Hlc, Origin, VersionVector};
use redb::{ReadableDatabase, ReadableTable};

use crate::error::{Result, StorageError};
use crate::tables;

/// One writer's record in a row: the origin's bytes, then the stamp's.
const RECORD: usize = Origin::LEN + kimmy_core::HLC_ENCODED_LEN;

/// The table, as the functions below take it.
pub(crate) type Rows<'txn> = redb::Table<'txn, (u64, &'static [u8]), &'static [u8]>;

fn corrupt() -> StorageError {
    StorageError::Corrupt("a missing-through row is not a whole number of records".into())
}

fn decode(bytes: &[u8]) -> Result<VersionVector> {
    if !bytes.len().is_multiple_of(RECORD) {
        return Err(corrupt());
    }
    let mut vector = VersionVector::new();
    for record in bytes.as_chunks::<RECORD>().0 {
        let (origin, hlc) = record.split_at(Origin::LEN);
        let origin = Origin::from_bytes(origin.try_into().expect("an origin's width"));
        let hlc = Hlc::from_bytes(hlc.try_into().expect("a stamp's width"));
        vector.insert(origin, hlc);
    }
    Ok(vector)
}

fn encode(vector: &VersionVector) -> Vec<u8> {
    let mut out = Vec::with_capacity(vector.len() * RECORD);
    for (origin, hlc) in vector.iter() {
        out.extend_from_slice(&origin.to_bytes());
        out.extend_from_slice(&hlc.to_bytes());
    }
    out
}

/// Raise the row of `key` in `collection` to `writes`, per origin: a stamp not above what the row holds changes
/// nothing, and a row that does not change is not written.
pub(crate) fn raise(
    rows: &mut Rows<'_>,
    collection: u64,
    key: &[u8],
    writes: impl IntoIterator<Item = (Origin, Hlc)>,
) -> Result<()> {
    let mut held = match rows.get((collection, key))? {
        Some(raw) => decode(raw.value())?,
        None => VersionVector::new(),
    };
    let mut changed = false;
    for (origin, hlc) in writes {
        if hlc > held.get(origin) {
            held.insert(origin, hlc);
            changed = true;
        }
    }
    if changed {
        rows.insert((collection, key), encode(&held).as_slice())?;
    }
    Ok(())
}

/// [`raise`] in a write transaction that has the table closed.
pub(crate) fn raise_in_txn(
    txn: &redb::WriteTransaction,
    collection: u64,
    key: &[u8],
    writes: impl IntoIterator<Item = (Origin, Hlc)>,
) -> Result<()> {
    let mut rows = txn.open_table(tables::MISSING_THROUGH)?;
    raise(&mut rows, collection, key, writes)
}

/// The row of `key` in `collection`: what this member no longer holds as an entry, per writer.
pub(crate) fn read(
    rows: &impl redb::ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    collection: u64,
    key: &[u8],
) -> Result<VersionVector> {
    match rows.get((collection, key))? {
        Some(raw) => decode(raw.value()),
        None => Ok(VersionVector::new()),
    }
}

/// Remove the row of `key` in `collection` (the document's tombstone was collected).
pub(crate) fn remove_in_txn(
    txn: &redb::WriteTransaction,
    collection: u64,
    key: &[u8],
) -> Result<()> {
    txn.open_table(tables::MISSING_THROUGH)?.remove((collection, key))?;
    Ok(())
}

/// Remove up to `limit` rows of a purged collection, and say how many went: the drop purger asks again while it
/// is as many as it asked for.
pub(crate) fn remove_collection_chunk(
    txn: &redb::WriteTransaction,
    collection: u64,
    limit: usize,
) -> Result<usize> {
    let mut rows = txn.open_table(tables::MISSING_THROUGH)?;
    let keys: Vec<Vec<u8>> = {
        let mut keys = Vec::new();
        for row in
            rows.range(crate::engine::doc_range(kimmy_core::CollectionId(collection)))?.take(limit)
        {
            let (key, _) = row?;
            keys.push(key.value().1.to_vec());
        }
        keys
    };
    for key in &keys {
        rows.remove((collection, key.as_slice()))?;
    }
    Ok(keys.len())
}

impl crate::Engine {
    /// What this member no longer holds as an entry of the document `id` of `collection`: per writer, the newest
    /// stamp of that writer's write to it that was collected or was only ever held as a state. Empty for a
    /// document of a `document` collection and for one every entry of which is held.
    pub fn missing_through(
        &self,
        collection: &crate::CollectionMeta,
        id: &kimmy_core::DocId,
    ) -> Result<VersionVector> {
        let key = crate::docs::doc_key(id)?;
        let txn = self.db().begin_read()?;
        let rows = txn.open_table(tables::MISSING_THROUGH)?;
        read(&rows, collection.id.0, &key)
    }
}
