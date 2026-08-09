//! Online backup, and offline restore.
//!
//! # Why not "copy the file"
//!
//! Copying `kimmy.redb` while a node is running copies a torn file: redb is
//! writing pages underneath the copy, and what lands on the other end is not any
//! state the database was ever in. The only safe cold copy is one taken with the
//! node stopped, which is what the operations guide had to recommend before this
//! existed.
//!
//! A backup taken here runs inside a **read transaction**, so redb's MVCC gives
//! it one consistent snapshot for the whole walk — every table is read as of the
//! same instant, and concurrent writers are unaffected and unblocked.
//!
//! # What a backup contains
//!
//! Every table, including secondary index entries. Index entries are derivable
//! from documents, so omitting them would make backups smaller — and would make
//! a restore's correctness depend on replaying index maintenance exactly, which
//! is the part most likely to change between versions. Copying them keeps a
//! restore a *transcription* rather than a re-derivation.
//!
//! It also carries the node identity. That is deliberate and has a sharp edge —
//! see [`restore`].
//!
//! # Format
//!
//! An explicit byte layout with a leading version, for the reason ADR-003 gives
//! for the on-disk records: a backup that a future version cannot read is a
//! backup that does not exist, and a format defined by a serde derive changes
//! when a dependency does.
//!
//! ```text
//!   magic "KIMMYBK1"     8 bytes
//!   format version       u8
//!   node id              16 bytes
//!   created (ms)         u64 big-endian
//!   records              (tag u8, key len u32, key, value len u32, value)*
//!   end                  tag 0xFF
//! ```
//!
//! Keys are written as their raw component bytes and reassembled on restore, so
//! the format does not depend on redb's key encoding staying stable either.

use std::io::{Read, Write};
use std::path::Path;

use kimmy_core::NodeId;
use redb::{Database, ReadableDatabase, ReadableTable};
use tracing::info;

use crate::engine::Engine;
use crate::error::{Result, StorageError};
use crate::tables;

const MAGIC: &[u8; 8] = b"KIMMYBK1";
const FORMAT: u8 = 1;
const END: u8 = 0xFF;

// One tag per table. Never reuse a number for a different table: a backup
// written by an older version has to keep meaning what it meant.
const T_META: u8 = 1;
const T_DATABASES: u8 = 2;
const T_COLLECTIONS: u8 = 3;
const T_DOCS: u8 = 4;
const T_INDEX_ENTRIES: u8 = 5;
const T_OPLOG: u8 = 6;
const T_OPLOG_ARRIVAL: u8 = 7;
const T_OPLOG_ARRIVAL_SEQ: u8 = 8;
const T_COLLECTIONS_DROPPED: u8 = 9;
const T_OPLOG_VERSIONS: u8 = 10;

/// What a backup or restore moved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BackupInfo {
    pub node: Option<NodeId>,
    pub created_ms: u64,
    pub records: usize,
    pub bytes: usize,
}

fn write_record(out: &mut impl Write, tag: u8, key: &[u8], value: &[u8]) -> Result<usize> {
    let io = |e: std::io::Error| StorageError::Database(format!("writing a backup: {e}"));
    out.write_all(&[tag]).map_err(io)?;
    out.write_all(&(key.len() as u32).to_be_bytes()).map_err(io)?;
    out.write_all(key).map_err(io)?;
    out.write_all(&(value.len() as u32).to_be_bytes()).map_err(io)?;
    out.write_all(value).map_err(io)?;
    Ok(1 + 4 + key.len() + 4 + value.len())
}

/// Length-prefix a component so a composite key can be split again.
fn push_part(out: &mut Vec<u8>, part: &[u8]) {
    out.extend_from_slice(&(part.len() as u32).to_be_bytes());
    out.extend_from_slice(part);
}

fn take_part<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    if input.len() < 4 {
        return Err(StorageError::Database("truncated key in backup".into()));
    }
    let (len, rest) = input.split_at(4);
    let len = u32::from_be_bytes(len.try_into().expect("4 bytes")) as usize;
    if rest.len() < len {
        return Err(StorageError::Database("truncated key component in backup".into()));
    }
    let (part, rest) = rest.split_at(len);
    *input = rest;
    Ok(part)
}

impl Engine {
    /// Stream a consistent backup.
    ///
    /// Runs in a read transaction, so writers keep working and every table is
    /// read as of the same instant.
    pub fn backup_to(&self, out: &mut impl Write) -> Result<BackupInfo> {
        let io = |e: std::io::Error| StorageError::Database(format!("writing a backup: {e}"));
        let created_ms = crate::engine::physical_now_ms();
        let node = self.node_id();

        out.write_all(MAGIC).map_err(io)?;
        out.write_all(&[FORMAT]).map_err(io)?;
        out.write_all(&node.to_bytes()).map_err(io)?;
        out.write_all(&created_ms.to_be_bytes()).map_err(io)?;
        let mut bytes = 8 + 1 + 16 + 8;
        let mut records = 0usize;

        // One read transaction for the whole walk. This is the line that makes
        // the backup consistent rather than a series of unrelated reads.
        let txn = self.db().begin_read()?;

        macro_rules! simple {
            ($tag:expr, $table:expr, $keybytes:expr) => {{
                match txn.open_table($table) {
                    Ok(t) => {
                        for entry in t.iter()? {
                            let (k, v) = entry?;
                            #[allow(clippy::redundant_closure_call)]
                            let key = ($keybytes)(k.value());
                            bytes += write_record(out, $tag, &key, v.value())?;
                            records += 1;
                        }
                    }
                    // A table absent from a database that never used it is not
                    // an error; a fresh node has never written some of these.
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(e) => return Err(e.into()),
                }
            }};
        }

        simple!(T_META, tables::META, |k: &str| k.as_bytes().to_vec());
        simple!(T_DATABASES, tables::DATABASES, |k: &str| k.as_bytes().to_vec());
        simple!(T_COLLECTIONS, tables::COLLECTIONS, |k: (&str, &str)| {
            let mut out = Vec::new();
            push_part(&mut out, k.0.as_bytes());
            push_part(&mut out, k.1.as_bytes());
            out
        });
        simple!(T_DOCS, tables::DOCS, |k: (u64, &[u8])| {
            let mut out = k.0.to_be_bytes().to_vec();
            out.extend_from_slice(k.1);
            out
        });
        simple!(T_OPLOG, tables::OPLOG, <[u8]>::to_vec);
        simple!(T_OPLOG_ARRIVAL, tables::OPLOG_ARRIVAL, |k: u64| k.to_be_bytes().to_vec());
        simple!(T_COLLECTIONS_DROPPED, tables::COLLECTIONS_DROPPED, |k: u64| k
            .to_be_bytes()
            .to_vec());
        simple!(T_OPLOG_VERSIONS, tables::OPLOG_VERSIONS, <[u8]>::to_vec);

        // Value types that are not `&[u8]` need their own arm.
        match txn.open_table(tables::OPLOG_ARRIVAL_SEQ) {
            Ok(t) => {
                for entry in t.iter()? {
                    let (k, v) = entry?;
                    bytes += write_record(
                        out,
                        T_OPLOG_ARRIVAL_SEQ,
                        k.value(),
                        &v.value().to_be_bytes(),
                    )?;
                    records += 1;
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
        }

        match txn.open_table(tables::INDEX_ENTRIES) {
            Ok(t) => {
                for entry in t.iter()? {
                    let (k, _) = entry?;
                    let (coll, index, key, id) = k.value();
                    let mut composed = Vec::new();
                    composed.extend_from_slice(&coll.to_be_bytes());
                    composed.extend_from_slice(&index.to_be_bytes());
                    push_part(&mut composed, key);
                    push_part(&mut composed, id);
                    bytes += write_record(out, T_INDEX_ENTRIES, &composed, &[])?;
                    records += 1;
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
        }

        out.write_all(&[END]).map_err(io)?;
        out.flush().map_err(io)?;
        bytes += 1;

        info!(records, bytes, %node, "wrote a backup");
        Ok(BackupInfo { node: Some(node), created_ms, records, bytes })
    }
}

/// Restore a backup into a **new** database file.
///
/// # Offline, and into a fresh path
///
/// redb allows one process to hold a database, so a restore cannot run against
/// a live node. Requiring a path that does not exist is deliberate on top of
/// that: a restore that overwrites in place turns a mistyped filename into data
/// loss, and the operator who wanted an overwrite can remove the file
/// themselves, having thought about it.
///
/// # The node identity comes back with it
///
/// The identity is the tiebreak half of every write's stamp, so a node that
/// restores under a *new* identity becomes a stranger to its own history:
/// last-writer-wins comparisons against its old writes change meaning. The
/// backup therefore carries it and a restore keeps it.
///
/// The sharp edge is that restoring one backup onto **two** nodes puts the same
/// identity on both, and the cluster then has two members it cannot tell apart
/// — which breaks the tiebreak that makes convergence deterministic. Restore is
/// for replacing a node, not for cloning one. Cloning needs a fresh identity,
/// and there is deliberately no flag for it here: it would be one keystroke
/// between "recover" and "corrupt the cluster's identity space".
pub fn restore(path: &Path, input: &mut impl Read) -> Result<BackupInfo> {
    if path.exists() {
        return Err(StorageError::Database(format!(
            "{} already exists; restore writes a new database rather than overwriting one",
            path.display()
        )));
    }

    let io = |e: std::io::Error| StorageError::Database(format!("reading a backup: {e}"));

    // The magic is checked before anything else is read, and separately, so
    // that a file which is simply *not a backup* says so. Reading the whole
    // header first meant a short file failed with "failed to fill whole
    // buffer" — accurate, useless, and the likeliest mistake here is pointing
    // at the wrong file rather than a corrupt one.
    let mut magic = [0u8; 8];
    input.read_exact(&mut magic).map_err(|_| {
        StorageError::Database(
            "not a KimmyDB backup: the file is too short to have a header".into(),
        )
    })?;
    if &magic != MAGIC {
        return Err(StorageError::Database(
            "not a KimmyDB backup (bad magic); check the file is the one you meant".into(),
        ));
    }

    let mut header = [0u8; 1 + 16 + 8];
    input.read_exact(&mut header).map_err(io)?;
    let header = {
        let mut full = [0u8; 8 + 1 + 16 + 8];
        full[..8].copy_from_slice(&magic);
        full[8..].copy_from_slice(&header);
        full
    };
    if header[8] != FORMAT {
        return Err(StorageError::Database(format!(
            "backup format version {} is not supported by this build (expected {FORMAT})",
            header[8]
        )));
    }
    let node = NodeId::from_bytes(header[9..25].try_into().expect("16 bytes"));
    let created_ms = u64::from_be_bytes(header[25..33].try_into().expect("8 bytes"));

    let db = Database::create(path)?;
    let mut records = 0usize;
    let mut bytes = header.len();

    {
        let txn = db.begin_write()?;
        // Opened up front so that an empty backup still produces a database
        // with the shape a node expects, rather than one missing tables.
        {
            let mut meta = txn.open_table(tables::META)?;
            let mut databases = txn.open_table(tables::DATABASES)?;
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            let mut docs = txn.open_table(tables::DOCS)?;
            let mut index_entries = txn.open_table(tables::INDEX_ENTRIES)?;
            let mut oplog = txn.open_table(tables::OPLOG)?;
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let mut arrival_seq = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;

            loop {
                let mut tag = [0u8; 1];
                input.read_exact(&mut tag).map_err(io)?;
                if tag[0] == END {
                    break;
                }
                let key = read_chunk(input)?;
                let value = read_chunk(input)?;
                bytes += 1 + 8 + key.len() + value.len();
                records += 1;

                match tag[0] {
                    T_META => {
                        meta.insert(as_str(&key)?, value.as_slice())?;
                    }
                    T_DATABASES => {
                        databases.insert(as_str(&key)?, value.as_slice())?;
                    }
                    T_COLLECTIONS => {
                        let mut rest = key.as_slice();
                        let db_name = take_part(&mut rest)?.to_vec();
                        let coll = take_part(&mut rest)?.to_vec();
                        collections
                            .insert((as_str(&db_name)?, as_str(&coll)?), value.as_slice())?;
                    }
                    T_DOCS => {
                        let (id, rest) = split_u64(&key)?;
                        docs.insert((id, rest), value.as_slice())?;
                    }
                    T_INDEX_ENTRIES => {
                        let (coll, rest) = split_u64(&key)?;
                        if rest.len() < 4 {
                            return Err(StorageError::Database("truncated index key".into()));
                        }
                        let (idx, mut rest) = rest.split_at(4);
                        let index_id = u32::from_be_bytes(idx.try_into().expect("4 bytes"));
                        let entry_key = take_part(&mut rest)?;
                        let doc_id = take_part(&mut rest)?;
                        index_entries.insert((coll, index_id, entry_key, doc_id), ())?;
                    }
                    T_OPLOG => {
                        oplog.insert(key.as_slice(), value.as_slice())?;
                    }
                    T_OPLOG_ARRIVAL => {
                        let (seq, _) = split_u64(&key)?;
                        arrival.insert(seq, value.as_slice())?;
                    }
                    T_OPLOG_ARRIVAL_SEQ => {
                        if value.len() != 8 {
                            return Err(StorageError::Database("bad arrival sequence".into()));
                        }
                        let seq = u64::from_be_bytes(value.as_slice().try_into().expect("8 bytes"));
                        arrival_seq.insert(key.as_slice(), seq)?;
                    }
                    T_COLLECTIONS_DROPPED => {
                        let (id, _) = split_u64(&key)?;
                        dropped.insert(id, value.as_slice())?;
                    }
                    T_OPLOG_VERSIONS => {
                        versions.insert(key.as_slice(), value.as_slice())?;
                    }
                    // A tag this build does not know means the backup came from
                    // a newer version. Skipping it would restore a database
                    // missing whatever that table held, silently.
                    other => {
                        return Err(StorageError::Database(format!(
                            "backup contains table {other}, which this build does not know; \
                             restore with the version that wrote it"
                        )));
                    }
                }
            }
        }
        txn.commit()?;
    }

    info!(records, bytes, %node, path = %path.display(), "restored a backup");
    Ok(BackupInfo { node: Some(node), created_ms, records, bytes })
}

fn read_chunk(input: &mut impl Read) -> Result<Vec<u8>> {
    let mut len = [0u8; 4];
    input
        .read_exact(&mut len)
        .map_err(|e| StorageError::Database(format!("reading a backup: {e}")))?;
    let len = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; len];
    input
        .read_exact(&mut buf)
        .map_err(|e| StorageError::Database(format!("truncated backup: {e}")))?;
    Ok(buf)
}

fn as_str(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| StorageError::Database("a name in the backup is not valid UTF-8".into()))
}

fn split_u64(key: &[u8]) -> Result<(u64, &[u8])> {
    if key.len() < 8 {
        return Err(StorageError::Database("truncated composite key in backup".into()));
    }
    let (head, rest) = key.split_at(8);
    Ok((u64::from_be_bytes(head.try_into().expect("8 bytes")), rest))
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::{DocId, index_meta::IndexField};

    use super::*;

    fn field(path: &str) -> IndexField {
        IndexField { path: path.to_string(), descending: false }
    }

    /// An engine holding a bit of everything a backup has to carry.
    fn populated() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("shop", "orders").unwrap();
        engine.create_index("shop", "orders", vec![field("sku")], true, None).unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        for i in 0..25i64 {
            engine.insert(&coll, doc! { "_id": i, "sku": format!("SKU-{i}"), "qty": i }).unwrap();
        }
        // A delete, so a tombstone is in the backup too.
        engine.delete(&coll, &DocId::Int64(3)).unwrap();
        // A dropped collection, so its tombstone is as well.
        engine.create_collection("shop", "scratch").unwrap();
        engine.drop_collection("shop", "scratch").unwrap();
        (engine, dir)
    }

    #[test]
    fn a_backup_round_trips_documents_indexes_and_identity() {
        let (engine, _dir) = populated();
        let coll = engine.get_collection("shop", "orders").unwrap();
        let before_count = engine.count(&coll).unwrap();
        let before_node = engine.node_id();
        let before_versions = engine.version_vector().unwrap();

        let mut buf = Vec::new();
        let info = engine.backup_to(&mut buf).unwrap();
        assert!(info.records > 0);
        assert_eq!(info.node, Some(before_node));

        let dest = tempfile::tempdir().unwrap();
        let path = dest.path().join("restored.redb");
        let restored_info = restore(&path, &mut buf.as_slice()).unwrap();
        assert_eq!(restored_info.records, info.records, "every record must come back");

        let restored = Engine::open(&path).unwrap();
        let rcoll = restored.get_collection("shop", "orders").expect("the collection must restore");

        assert_eq!(restored.count(&rcoll).unwrap(), before_count, "document count");
        assert_eq!(
            restored.node_id(),
            before_node,
            "identity is the tiebreak half of every stamp; a restored node that forgets it \
             becomes a stranger to its own writes"
        );
        assert_eq!(
            restored.version_vector().unwrap(),
            before_versions,
            "the version vector must survive, or the node re-requests history it already has"
        );

        // The index has to be usable, not merely present.
        let index = rcoll.indexes.iter().find(|i| i.name == "sku_1").expect("index restored");
        assert!(index.unique, "uniqueness must survive");
        let candidates =
            restored.index_candidates(&rcoll, index.id, &[], &[0xff, 0xff, 0xff, 0xff]).unwrap();
        assert!(!candidates.is_empty(), "index entries must be restored, not just the definition");
    }

    #[test]
    fn a_deleted_document_stays_deleted_after_a_restore() {
        // Tombstones are the thing whose absence is invisible until a
        // partitioned peer resurrects the document. A restore that dropped them
        // would look perfectly healthy.
        let (engine, _dir) = populated();
        let mut buf = Vec::new();
        engine.backup_to(&mut buf).unwrap();

        let dest = tempfile::tempdir().unwrap();
        let path = dest.path().join("restored.redb");
        restore(&path, &mut buf.as_slice()).unwrap();

        let restored = Engine::open(&path).unwrap();
        let rcoll = restored.get_collection("shop", "orders").unwrap();
        assert!(
            restored.get(&rcoll, &DocId::Int64(3)).unwrap().is_none(),
            "a deleted document must not come back"
        );
        assert!(
            restored.get_collection("shop", "scratch").is_err(),
            "a dropped collection must not come back"
        );
    }

    #[test]
    fn a_backup_is_consistent_while_writes_continue() {
        // The point of taking it in a read transaction. Without one, a backup
        // taken under load is a mix of states the database was never in.
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        engine.create_collection("shop", "orders").unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        for i in 0..50i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        let writer = {
            let engine = std::sync::Arc::clone(&engine);
            std::thread::spawn(move || {
                let coll = engine.get_collection("shop", "orders").unwrap();
                for i in 1_000..1_200i64 {
                    engine.insert(&coll, doc! { "_id": i }).unwrap();
                }
            })
        };

        let mut buf = Vec::new();
        engine.backup_to(&mut buf).unwrap();
        writer.join().unwrap();

        let dest = tempfile::tempdir().unwrap();
        let path = dest.path().join("restored.redb");
        restore(&path, &mut buf.as_slice()).unwrap();
        let restored = Engine::open(&path).unwrap();
        let rcoll = restored.get_collection("shop", "orders").unwrap();

        // The snapshot may contain some, all or none of the concurrent writes —
        // what it must never be is internally inconsistent. Every document it
        // holds must be readable, and the original 50 must all be present.
        for i in 0..50i64 {
            assert!(
                restored.get(&rcoll, &DocId::Int64(i)).unwrap().is_some(),
                "document {i} was committed before the backup began"
            );
        }
        let count = restored.count(&rcoll).unwrap();
        assert!((50..=250).contains(&count), "implausible count {count}");
    }

    #[test]
    fn restore_refuses_to_overwrite() {
        // A restore that overwrites in place turns a mistyped path into data
        // loss, and the operator who wants one can remove the file themselves.
        let (engine, _dir) = populated();
        let mut buf = Vec::new();
        engine.backup_to(&mut buf).unwrap();

        let dest = tempfile::tempdir().unwrap();
        let path = dest.path().join("restored.redb");
        std::fs::write(&path, b"something already here").unwrap();

        let err = restore(&path, &mut buf.as_slice()).unwrap_err().to_string();
        assert!(err.contains("already exists"), "unhelpful error: {err}");
    }

    #[test]
    fn a_file_that_is_not_a_backup_is_refused_by_name() {
        let dest = tempfile::tempdir().unwrap();
        let err = restore(&dest.path().join("x.redb"), &mut b"not a backup at all".as_slice())
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a KimmyDB backup"), "unhelpful error: {err}");
    }

    #[test]
    fn a_truncated_backup_is_refused_rather_than_half_restored() {
        // The worst outcome would be a database that opens and is quietly
        // missing whatever came after the truncation.
        let (engine, _dir) = populated();
        let mut buf = Vec::new();
        engine.backup_to(&mut buf).unwrap();
        buf.truncate(buf.len() / 2);

        let dest = tempfile::tempdir().unwrap();
        let path = dest.path().join("restored.redb");
        assert!(
            restore(&path, &mut buf.as_slice()).is_err(),
            "a truncated backup must not restore"
        );
    }

    #[test]
    fn a_backup_from_a_newer_format_is_refused() {
        // Skipping an unknown table would restore a database silently missing
        // whatever it held.
        let (engine, _dir) = populated();
        let mut buf = Vec::new();
        engine.backup_to(&mut buf).unwrap();
        buf[8] = FORMAT + 1;

        let dest = tempfile::tempdir().unwrap();
        let err =
            restore(&dest.path().join("r.redb"), &mut buf.as_slice()).unwrap_err().to_string();
        assert!(err.contains("format version"), "unhelpful error: {err}");
    }
}
