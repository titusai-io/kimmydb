//! The storage schema.
//!
//! The record encoding ([`crate::codec::FORMAT_VERSION`]) and the database
//! *layout* are separate concerns and version separately. A record whose bytes
//! still decode does not need rewriting because the meaning of a key changed
//! around it, which is the situation schema 6 marks.
//!
//! This build writes one schema and opens only a store of that schema. A store
//! of another schema is refused by name, with nothing written: an older one
//! because this build does not upgrade stores, a newer one because a newer
//! build wrote it.

use redb::{Database, ReadableDatabase};

use crate::error::{RefusedKind, Result, StorageError};
use crate::tables;

/// The database layout this build writes and understands.
///
/// **6 (ADR-251) changes what a binary `_id`'s key means.** The key was always
/// `0x60 || length || subtype || bytes`, but the subtype byte was always 0; it
/// is now the id's own. An older store's binary keys mean something else, and
/// nothing rewrites them: this build refuses an older store by name and the
/// operator wipes and reseeds.
///
/// **7 (ADR-259) changes how a compound index counts the arrays its paths go
/// through.** A schema-6 store holds index entries for documents the new rule
/// files unkeyed, and a later write to one would leave them behind. Nothing
/// rewrites them: this build refuses a schema-6 store and backup by name.
///
/// **8 (Q7 step 1) puts the epoch in the stamp.** A stamp is `(hlc, node, epoch)`, 44 bytes in
/// every key and value that held 26, and the version vectors, the tables behind them, the
/// digest rows, the resume tokens and the rewind record are keyed by origin (node and epoch).
/// The `OPLOG` value no longer carries its stamp, and a new `epoch` META row holds the member's
/// current epoch. Nothing rewrites an older store: this build refuses a schema-7 store and
/// backup by name.
///
/// **9 was written only by builds that never shipped.** The series that changed the keys of an
/// index and of a document value took 8 and 9 before Q7 step 1 took 8 for the epoch; it was
/// renumbered at the merge (10 and 11). This build refuses a schema-9 store and backup by name.
///
/// **10 changes the keys an index derives from a path through arrays, and, with the
/// document order of the same series, the key a document value encodes to.** An
/// element lacking the field, a scalar element and an empty array on the path key
/// `null`; a position inside the array keys that element only; `00` and `01` are
/// field names; and a document value orders by value type, then name, then value. A
/// store of an older schema holds entries the new rule would not write, and nothing
/// rewrites them: this build refuses it and its backup by name.
///
/// **11 changes the key of a document value**: a field is its type tag, its name, then its value
/// without the tag, so documents order by the type of their values first. An index over documents
/// (and a key that holds one, an array of them included, and a JavaScript value with a scope)
/// written under schema 10 orders otherwise; this build refuses a schema-10 store and backup by
/// name. The commits of the series between schema 10 and 11 are not deployable.
pub const SCHEMA_VERSION: u8 = 11;

/// What a refusal of a store or backup of an older schema tells the operator.
pub(crate) fn no_upgrade() -> String {
    format!(
        "{} does not upgrade stores; stop every member, wipe this data directory, start on an \
         empty one and reseed.",
        kimmy_core::build::SCHEMA_11_RELEASE
    )
}

/// Stamp a fresh database, or refuse one of another schema. `raise` records in
/// the store's sidecar that its schema is about to be written (ADR-190),
/// before a fresh store's version is written. `path` is the store's file.
pub(crate) fn run(
    db: &Database,
    path: &std::path::Path,
    raise: &dyn Fn(u8) -> Result<()>,
) -> Result<()> {
    match stored_version(db)? {
        // A fresh database: nothing to migrate, just stamp it.
        None => {
            raise(SCHEMA_VERSION)?;
            write_version(db, SCHEMA_VERSION)
        }
        Some(SCHEMA_VERSION) => Ok(()),
        // Unreachable through `Engine::open`, which refuses any other schema in
        // `refuse_other_schema` first; kept as a guard for a caller that does not.
        Some(found) => Err(refuse_schema(found, path)),
    }
}

/// The refusal of a store whose schema is `found`, not this build's, read from
/// its META: the sidecar that names the writing build is not there to read.
fn refuse_schema(found: u8, path: &std::path::Path) -> StorageError {
    if found > SCHEMA_VERSION {
        return StorageError::UnsupportedFormat { found, expected: SCHEMA_VERSION };
    }
    StorageError::RefusedStore {
        kind: RefusedKind::Older,
        why: format!(
            "{} was written by a build that left no kimmy.format, with storage schema {found} \
             (this build: {SCHEMA_VERSION}). {}",
            path.display(),
            no_upgrade()
        ),
    }
}

/// Record the redb major.minor in META of a store that has none (ADR-190): run
/// on the raw database during `Engine::open`, so the read-only check can refuse
/// a clean store with no sidecar that another redb wrote.
///
/// **Never restamped.** A fresh store, and a restored one (which carries none of
/// its source's), records this build's. A store that records another minor,
/// older or newer, is refused here as it was before the open: the build's redb
/// does not open a store another minor wrote, and rewriting the record would
/// hide that from the next build to look.
pub(crate) fn record_redb_version(
    db: &Database,
    build: &crate::format::BuildVersions,
    path: &std::path::Path,
) -> Result<()> {
    let ours = format!("{}.{}", build.redb.0, build.redb.1);
    match stored_redb_version(db)? {
        // This build's minor, a patch of it included: not a boundary.
        Some(stored) if crate::format::major_minor(&stored) == Some(build.redb) => return Ok(()),
        Some(stored) => return Err(refuse_redb(&stored, path, build)),
        None => {}
    }
    let txn = db.begin_write()?;
    txn.open_table(tables::META)?.insert(crate::format::META_REDB_VERSION, ours.as_bytes())?;
    txn.commit()?;
    Ok(())
}

/// The refusal of a store whose recorded redb is not this build's minor.
fn refuse_redb(
    recorded: &str,
    path: &std::path::Path,
    build: &crate::format::BuildVersions,
) -> StorageError {
    match crate::format::major_minor(recorded) {
        Some(v) if v > build.redb => StorageError::RefusedStore {
            kind: RefusedKind::Newer,
            why: format!(
                "{} was last written by redb {recorded}, newer than this build's {}.{}. It had no \
                 kimmy.format and was not shut down cleanly, so redb repaired it before that \
                 could be read; nothing but redb's own repair and close was written. Start the \
                 build that wrote it, or restore a backup",
                path.display(),
                build.redb.0,
                build.redb.1
            ),
        },
        Some(_) => StorageError::RefusedStore {
            kind: RefusedKind::Older,
            why: format!(
                "{} was last written by redb {recorded}, older than this build's {}.{}. {}",
                path.display(),
                build.redb.0,
                build.redb.1,
                crate::format::redb_no_upgrade(build)
            ),
        },
        None => StorageError::RefusedStore {
            kind: RefusedKind::Damaged,
            why: format!(
                "{} records the redb version {recorded:?}, which is not major.minor",
                path.display()
            ),
        },
    }
}

/// The schema META records, `None` for a store with no META yet.
pub(crate) fn stored_version(db: &Database) -> Result<Option<u8>> {
    let txn = db.begin_read()?;
    let meta = match txn.open_table(tables::META) {
        Ok(meta) => meta,
        // A fresh store, before the tables exist.
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(meta.get(tables::META_FORMAT_VERSION)?.and_then(|v| v.value().first().copied()))
}

/// Refuse a store of another schema, reading only, before the ensure-tables
/// commit (ADR-190): a newer or an older schema, or another redb minor recorded
/// in META. The check before the open already refused every store it could read.
/// What reaches here is a dirty store with no sidecar, which redb has just
/// repaired: the repair wrote, which is the gap ADR-190 documents, but the
/// session stops here, and META still says which schema and redb wrote the store.
pub(crate) fn refuse_other_schema(
    db: &Database,
    path: &std::path::Path,
    build: &crate::format::BuildVersions,
) -> Result<()> {
    match stored_version(db)? {
        Some(found) if found != SCHEMA_VERSION => {
            return Err(refuse_schema(found, path));
        }
        _ => {}
    }
    match stored_redb_version(db)? {
        Some(recorded) if recorded != format!("{}.{}", build.redb.0, build.redb.1) => {
            // A patch recorded is not a boundary: compare major.minor.
            if crate::format::major_minor(&recorded) != Some(build.redb) {
                return Err(refuse_redb(&recorded, path, build));
            }
        }
        // A store with a schema and no redb version was not written by this
        // build, which records it on every open: damaged, not stamped.
        None if stored_version(db)?.is_some() => {
            return Err(StorageError::RefusedStore {
                kind: RefusedKind::Damaged,
                why: format!("{} records a storage schema and no redb version", path.display()),
            });
        }
        _ => {}
    }
    Ok(())
}

/// The redb major.minor META records, tolerating a missing META.
fn stored_redb_version(db: &Database) -> Result<Option<String>> {
    let txn = db.begin_read()?;
    let meta = match txn.open_table(tables::META) {
        Ok(meta) => meta,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(meta
        .get(crate::format::META_REDB_VERSION)?
        .and_then(|v| std::str::from_utf8(v.value()).ok().map(str::to_string)))
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

#[cfg(test)]
mod tests {
    use redb::Database;

    use super::*;
    use crate::Engine;
    use crate::format::{BuildVersions, read_sidecar, sidecar_path, write_sidecar};

    /// A closed store left as a build of schema 5 leaves it: META and the
    /// sidecar at 5, the sidecar naming 0.52.0.
    // A closed store rewritten by a test, on purpose, outside the check
    // `Engine::open` makes.
    #[allow(clippy::disallowed_methods)]
    fn a_schema_5_store() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let c = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&c, bson::doc! { "_id": 1 }).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            write_version(&db, 5).unwrap();
        }
        let mut sidecar = read_sidecar(&path).expect("a sidecar");
        sidecar.schema = 5;
        sidecar.written_by = "0.52.0".into();
        write_sidecar(&path, &sidecar).unwrap();
        (dir, path)
    }

    fn snapshot(path: &std::path::Path) -> (Vec<u8>, Option<Vec<u8>>) {
        (std::fs::read(path).unwrap(), std::fs::read(sidecar_path(path)).ok())
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

    /// The check before the open, on the sidecar: refused by name, and nothing
    /// is written.
    #[test]
    fn a_schema_5_store_is_refused_by_name_before_the_open_and_nothing_is_written() {
        let (_dir, path) = a_schema_5_store();
        let before = snapshot(&path);
        match Engine::open(&path) {
            Err(StorageError::RefusedStore { kind: RefusedKind::Older, why }) => {
                assert!(why.contains("0.52.0"), "{why}");
                assert!(why.contains("storage schema 5"), "{why}");
                assert!(why.contains("does not upgrade stores"), "{why}");
                assert!(why.contains("nothing in it was changed"), "{why}");
            }
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a schema 5 store"),
        }
        assert!(snapshot(&path) == before, "the store or its sidecar changed");
    }

    /// The store 0.57.0 wrote (schema 6) holds index entries derived under the
    /// old array rule (ADR-259): this build refuses it by name and writes nothing.
    #[test]
    fn a_schema_6_store_is_refused_by_name_and_nothing_is_written() {
        let (_dir, path) = a_schema_5_store();
        {
            let db = Database::create(&path).unwrap();
            write_version(&db, 6).unwrap();
        }
        let mut sidecar = read_sidecar(&path).expect("a sidecar");
        sidecar.schema = 6;
        sidecar.written_by = "0.57.0".into();
        write_sidecar(&path, &sidecar).unwrap();
        let before = snapshot(&path);
        match Engine::open(&path) {
            Err(StorageError::RefusedStore { kind: RefusedKind::Older, why }) => {
                assert!(why.contains("0.57.0"), "{why}");
                assert!(why.contains("storage schema 6"), "{why}");
                assert!(why.contains("0.61.0 does not upgrade stores"), "{why}");
            }
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a schema 6 store"),
        }
        assert!(snapshot(&path) == before, "the store or its sidecar changed");
    }

    /// The store 0.60.0 wrote (schema 7) holds index entries keyed under the old path rule
    /// and document order: this build refuses it by name and writes nothing.
    #[test]
    fn a_schema_7_store_is_refused_by_name_and_nothing_is_written() {
        let (_dir, path) = a_schema_5_store();
        {
            let db = Database::create(&path).unwrap();
            write_version(&db, 7).unwrap();
        }
        let mut sidecar = read_sidecar(&path).expect("a sidecar");
        sidecar.schema = 7;
        sidecar.written_by = "0.60.0".into();
        write_sidecar(&path, &sidecar).unwrap();
        let before = snapshot(&path);
        match Engine::open(&path) {
            Err(StorageError::RefusedStore { kind: RefusedKind::Older, why }) => {
                assert!(why.contains("0.60.0"), "{why}");
                assert!(why.contains("storage schema 7"), "{why}");
                assert!(why.contains("0.61.0 does not upgrade stores"), "{why}");
            }
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a schema 7 store"),
        }
        assert!(snapshot(&path) == before, "the store or its sidecar changed");
    }

    /// A store a build of this release wrote under schema `n` (a build between the changes of
    /// the stamp, the index keys and the document keys): this build refuses it by name and
    /// writes nothing.
    fn assert_a_release_build_schema_is_refused(n: u8) {
        let (_dir, path) = a_schema_5_store();
        {
            let db = Database::create(&path).unwrap();
            write_version(&db, n).unwrap();
        }
        let mut sidecar = read_sidecar(&path).expect("a sidecar");
        sidecar.schema = n;
        sidecar.written_by = "0.61.0".into();
        write_sidecar(&path, &sidecar).unwrap();
        let before = snapshot(&path);
        match Engine::open(&path) {
            Err(StorageError::RefusedStore { kind: RefusedKind::Older, why }) => {
                assert!(why.contains("0.61.0"), "{why}");
                assert!(why.contains(&format!("storage schema {n}")), "{why}");
                assert!(why.contains("0.61.0 does not upgrade stores"), "{why}");
            }
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a schema {n} store"),
        }
        assert!(snapshot(&path) == before, "the store or its sidecar changed");
    }

    /// Schema 8 (the epoch in the stamp, Q7 step 1): the stamps are 26 bytes wide in the older
    /// layout, 44 in this one.
    #[test]
    fn a_schema_8_store_is_refused_by_name_and_nothing_is_written() {
        assert_a_release_build_schema_is_refused(8);
    }

    /// Schema 9 was written only by builds that never shipped.
    #[test]
    fn a_schema_9_store_is_refused_by_name_and_nothing_is_written() {
        assert_a_release_build_schema_is_refused(9);
    }

    /// Schema 10 holds document keys of the old order (the index keys changed, the document
    /// value's key did not yet).
    #[test]
    fn a_schema_10_store_is_refused_by_name_and_nothing_is_written() {
        assert_a_release_build_schema_is_refused(10);
    }

    /// With no sidecar the read-only fallback reads META: the same refusal.
    #[test]
    fn a_schema_5_store_with_no_sidecar_is_refused_by_name_and_nothing_is_written() {
        let (_dir, path) = a_schema_5_store();
        std::fs::remove_file(sidecar_path(&path)).unwrap();
        let before = snapshot(&path);
        match Engine::open(&path) {
            Err(StorageError::RefusedStore { kind: RefusedKind::Older, why }) => {
                assert!(why.contains("storage schema 5"), "{why}");
                assert!(why.contains("does not upgrade stores"), "{why}");
            }
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a schema 5 store"),
        }
        assert!(snapshot(&path) == before, "the store or its sidecar changed");
    }

    /// The check after the open, which a dirty store with no sidecar reaches:
    /// it reads META only.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn the_check_after_the_open_refuses_a_schema_5_store_by_name_and_writes_nothing() {
        let (_dir, path) = a_schema_5_store();
        {
            let db = Database::create(&path).unwrap();
            match refuse_other_schema(&db, &path, &BuildVersions::ours()) {
                Err(StorageError::RefusedStore { kind: RefusedKind::Older, why }) => {
                    assert!(why.contains("storage schema 5"), "{why}");
                    assert!(why.contains("does not upgrade stores"), "{why}");
                }
                other => panic!("expected an older-store refusal, got {other:?}"),
            }
            // Writes nothing: the version a build of schema 5 left is still there.
            assert_eq!(stored_version(&db).unwrap(), Some(5));
        }
    }

    #[test]
    #[allow(clippy::disallowed_methods)]
    fn a_fresh_store_is_stamped_with_this_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        drop(Engine::open(&path).unwrap());
        assert_eq!(SCHEMA_VERSION, 11);
        let db = Database::create(&path).unwrap();
        assert_eq!(stored_version(&db).unwrap(), Some(SCHEMA_VERSION));
    }
}
