//! The store is checked before anything opens it for writing (ADR-190).
//!
//! An older build refusing a newer store used to write to it first: redb's
//! read-write open repairs a dirty file and rewrites the header even on a clean
//! one, and the engine committed its ensure-tables transaction, all before
//! `migrate` read the format version and refused. Round 0380 saw the file change
//! on a killed store and on a cleanly stopped one.
//!
//! So a start now reads, and only reads, before it opens:
//!
//! 1. **The header**, 320 bytes: redb's magic and each commit slot's
//!    file-format byte. A format this build does not write is refused before
//!    any redb call.
//! 2. **The sidecar**, `kimmy.format` beside the database: the schema the
//!    store may hold, and the redb major.minor and file format that last
//!    opened it for writing. It is readable whether or not the store is dirty,
//!    which is the one thing redb's read-only open cannot do. Newer anywhere is
//!    refused; unreadable is refused and never guessed past.
//! 3. **With no sidecar**, redb's read-only open, which cannot write: the
//!    schema and the redb version recorded in META. A dirty store answers
//!    `RepairAborted`, and only that answer proceeds without a verdict: such a
//!    store was last written by a build before this one, whose versions are no
//!    newer than ours. Any other error refuses.
//!
//! Then the file is opened and locked, the header and sidecar are read again
//! through the lock and must be unchanged, and only then is anything written.
//! [`Cleared`] is what the check returns, and the read-write open requires it,
//! so no path reaches `create_with_backend` without the check.
//!
//! **A redb patch release is not a boundary.** redb keeps a patch within the
//! same file format and the same allocator and system-table rules; a change to
//! either comes in a major or minor release, and a format change also moves the
//! header byte, which is checked on its own. Dependabot bumps redb patches, and
//! each would otherwise be a rollback boundary for nothing.
//!
//! **Protection starts with the build that has this.** A build before it does
//! not read the sidecar, so rolling back to one still opens the store for
//! writing before it refuses.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::error::{Result, StorageError};
use crate::tables;

/// The redb major.minor this build is compiled against. `Cargo.toml` pins redb
/// to `~4.1`, so a patch release is the only kind that can arrive unnoticed; a
/// test holds this constant to `Cargo.lock`.
pub const REDB_MAJOR_MINOR: (u64, u64) = (4, 1);

/// The file-format byte redb 4.1 writes in each commit slot. A test creates a
/// store and reads it back.
pub const REDB_FILE_FORMAT: u8 = 3;

/// META key holding the redb major.minor that last opened the store for
/// writing, so the read-only fallback catches a newer redb on a clean store
/// with no sidecar.
pub(crate) const META_REDB_VERSION: &str = "redb_version";

const HEADER_LEN: usize = 320;
const MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const SLOT_OFFSETS: [usize; 2] = [64, 192];

/// The versions a build writes, and so the newest it may open. Injectable, so a
/// test can be an older build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildVersions {
    pub schema: u8,
    pub redb: (u64, u64),
    pub file_format: u8,
    pub written_by: String,
}

impl BuildVersions {
    /// This build's.
    pub fn ours() -> Self {
        BuildVersions {
            schema: crate::migrate::SCHEMA_VERSION,
            redb: REDB_MAJOR_MINOR,
            file_format: REDB_FILE_FORMAT,
            written_by: kimmy_core::build::VERSION.to_string(),
        }
    }
}

/// The sidecar's contents. Unknown fields are ignored, so a later build may add
/// one; a missing field refuses the store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sidecar {
    pub schema: u8,
    /// `major.minor`; a `major.minor.patch` is read, and its patch ignored.
    pub redb_version: String,
    pub redb_file_format: u8,
    pub written_by: String,
}

/// Where the sidecar of the store at `database` lives.
pub fn sidecar_path(database: &Path) -> PathBuf {
    database.with_extension("format")
}

/// What was read before the open, and what it said.
#[derive(Debug)]
enum Prior {
    /// No database, or an empty file.
    Fresh,
    /// A sidecar, no newer than this build.
    Sidecar(Sidecar),
    /// No sidecar, a clean store no newer than this build; its META schema.
    CleanNoSidecar { schema: Option<u8> },
    /// No sidecar, a dirty store: last written by a build before this one.
    DirtyNoSidecar,
}

/// The check passed. Only [`check_before_open_with`] makes one, and the engine's
/// read-write open takes it by value: there is no other way to that open.
#[derive(Debug)]
pub struct Cleared {
    prior: Prior,
    header: Option<Vec<u8>>,
    sidecar: Option<Vec<u8>>,
    pub(crate) build: BuildVersions,
}

/// [`check_before_open_with`], as this build.
pub fn check_before_open(database: &Path) -> Result<Cleared> {
    check_before_open_with(database, &BuildVersions::ours())
}

/// Read the store at `database` without writing to it, and refuse it if a newer
/// build wrote it (see the module docs).
pub fn check_before_open_with(database: &Path, build: &BuildVersions) -> Result<Cleared> {
    let sidecar_file = sidecar_path(database);
    // A temporary sidecar left by a crash mid-write is never read: the rename
    // either happened or it did not.
    remove_stale_temporaries(&sidecar_file);
    let len = match std::fs::metadata(database) {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    // A zero-length file is what a start that died before redb laid the file
    // out leaves: fresh. A sidecar beside no database is overwritten later.
    if len == 0 {
        return Ok(Cleared {
            prior: Prior::Fresh,
            header: None,
            sidecar: None,
            build: build.clone(),
        });
    }
    if (len as usize) < HEADER_LEN {
        return Err(refused(database, format!("the file is {len} bytes, shorter than a header")));
    }
    let header = read_header(&mut std::fs::File::open(database)?)?;
    check_header(database, &header, build)?;

    let sidecar_bytes = match std::fs::read(&sidecar_file) {
        Ok(bytes) => Some(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let prior = match &sidecar_bytes {
        Some(bytes) => {
            let sidecar = parse_sidecar(&sidecar_file, bytes)?;
            check_sidecar(database, &sidecar, build)?;
            Prior::Sidecar(sidecar)
        }
        None => read_only_fallback(database, build)?,
    };
    Ok(Cleared { prior, header: Some(header), sidecar: sidecar_bytes, build: build.clone() })
}

fn read_header(file: &mut std::fs::File) -> Result<Vec<u8>> {
    let mut header = vec![0u8; HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    Ok(header)
}

fn check_header(database: &Path, header: &[u8], build: &BuildVersions) -> Result<()> {
    if header[..MAGIC.len()] != MAGIC {
        return Err(refused(database, "the file does not begin with redb's magic".into()));
    }
    for offset in SLOT_OFFSETS {
        let format = header[offset];
        if format > build.file_format {
            return Err(refused(
                database,
                format!(
                    "redb file format {format} is newer than this build writes ({}); a build \
                     with a newer redb wrote it",
                    build.file_format
                ),
            ));
        }
    }
    Ok(())
}

fn parse_sidecar(path: &Path, bytes: &[u8]) -> Result<Sidecar> {
    let unreadable = |why: String| {
        StorageError::RefusedStore(format!(
            "{} is unreadable ({why}). It records which builds may open this store: restore it \
             from the source directory or the backup, or see operations.md; do not delete it",
            path.display()
        ))
    };
    let text = std::str::from_utf8(bytes).map_err(|e| unreadable(e.to_string()))?;
    let sidecar: Sidecar = toml::from_str(text).map_err(|e| unreadable(e.to_string()))?;
    major_minor(&sidecar.redb_version).ok_or_else(|| {
        unreadable(format!("redb_version {:?} is not major.minor", sidecar.redb_version))
    })?;
    Ok(sidecar)
}

/// `major.minor` from `major.minor` or `major.minor.patch`.
fn major_minor(version: &str) -> Option<(u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    match parts.next() {
        None => Some((major, minor)),
        Some(patch) if patch.parse::<u64>().is_ok() && parts.next().is_none() => {
            Some((major, minor))
        }
        Some(_) => None,
    }
}

fn check_sidecar(database: &Path, sidecar: &Sidecar, build: &BuildVersions) -> Result<()> {
    let redb = major_minor(&sidecar.redb_version).expect("parsed");
    let newer = if sidecar.schema > build.schema {
        Some(format!("storage schema {} (this build: {})", sidecar.schema, build.schema))
    } else if redb > build.redb {
        Some(format!("redb {}.{} (this build: {}.{})", redb.0, redb.1, build.redb.0, build.redb.1))
    } else if sidecar.redb_file_format > build.file_format {
        Some(format!(
            "redb file format {} (this build: {})",
            sidecar.redb_file_format, build.file_format
        ))
    } else {
        None
    };
    match newer {
        Some(what) => {
            Err(refused(database, format!("it was written by {} with {what}", sidecar.written_by)))
        }
        None => Ok(()),
    }
}

fn read_only_fallback(database: &Path, build: &BuildVersions) -> Result<Prior> {
    match redb::Builder::new().open_read_only(database) {
        Ok(db) => {
            let (schema, redb) = {
                let txn = db.begin_read()?;
                match txn.open_table(tables::META) {
                    Ok(meta) => {
                        let schema = meta
                            .get(tables::META_FORMAT_VERSION)?
                            .and_then(|v| v.value().first().copied());
                        let redb = meta
                            .get(META_REDB_VERSION)?
                            .and_then(|v| std::str::from_utf8(v.value()).ok().map(str::to_string));
                        (schema, redb)
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => (None, None),
                    Err(e) => return Err(e.into()),
                }
            };
            drop(db);
            if let Some(schema) = schema
                && schema > build.schema
            {
                return Err(refused(
                    database,
                    format!("storage schema {schema} (this build: {})", build.schema),
                ));
            }
            if let Some(version) = redb
                && major_minor(&version).is_some_and(|v| v > build.redb)
            {
                return Err(refused(
                    database,
                    format!(
                        "redb {version} last wrote it (this build: {}.{})",
                        build.redb.0, build.redb.1
                    ),
                ));
            }
            Ok(Prior::CleanNoSidecar { schema })
        }
        Err(redb::DatabaseError::RepairAborted) => Ok(Prior::DirtyNoSidecar),
        Err(redb::DatabaseError::DatabaseAlreadyOpen) => Err(in_use(database)),
        Err(e) => Err(refused(
            database,
            format!("it could not be read before opening it for writing: {e}"),
        )),
    }
}

/// What a start is told when another process has the store open.
pub(crate) fn in_use(database: &Path) -> StorageError {
    StorageError::StoreInUse(format!(
        "{} is open in another process, so this one does not open it and nothing in it was \
         changed",
        database.display()
    ))
}

fn refused(database: &Path, why: String) -> StorageError {
    StorageError::RefusedStore(format!(
        "{} is not opened by this build, and nothing in it was changed: {why}",
        database.display()
    ))
}

impl Cleared {
    /// Read the header and the sidecar again through the locked handle, and
    /// refuse if either changed since the check: another process got there in
    /// between.
    pub(crate) fn confirm_under_lock(&self, database: &Path, locked: &std::fs::File) -> Result<()> {
        let mut file = locked.try_clone()?;
        let len = file.metadata()?.len();
        let header = if self.header.is_some() {
            Some(read_header(&mut file)?)
        } else if len == 0 {
            None
        } else {
            return Err(refused(database, "it was created while it was being checked".into()));
        };
        let sidecar = match std::fs::read(sidecar_path(database)) {
            Ok(bytes) => Some(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let sidecar_matters = !matches!(self.prior, Prior::Fresh);
        if header != self.header || (sidecar_matters && sidecar != self.sidecar) {
            return Err(refused(database, "it changed while it was being checked".into()));
        }
        Ok(())
    }

    /// What to write before the read-write open, if anything: the schema is
    /// never raised here, only the redb fields brought to this build's; and a
    /// dirty store with no sidecar gets one only after it has opened.
    pub(crate) fn sidecar_before_open(&self) -> Option<Sidecar> {
        let schema = match &self.prior {
            // Nothing is protected yet, and a sidecar lying beside no database
            // is overwritten: the migration raises the schema from here.
            Prior::Fresh => return Some(self.stamp(0)),
            Prior::DirtyNoSidecar => return None,
            Prior::Sidecar(sidecar) => sidecar.schema,
            Prior::CleanNoSidecar { schema } => schema.unwrap_or(0),
        };
        let wanted = self.stamp(schema);
        match &self.prior {
            Prior::Sidecar(sidecar) if *sidecar == wanted => None,
            _ => Some(wanted),
        }
    }

    fn stamp(&self, schema: u8) -> Sidecar {
        Sidecar {
            schema,
            redb_version: format!("{}.{}", self.build.redb.0, self.build.redb.1),
            redb_file_format: self.build.file_format,
            written_by: self.build.written_by.clone(),
        }
    }
}

/// Write the sidecar atomically: a temporary file, synced, renamed, and the
/// directory synced.
pub(crate) fn write_sidecar(database: &Path, sidecar: &Sidecar) -> Result<()> {
    use std::io::Write;
    let path = sidecar_path(database);
    // Named for the sidecar and this process: no other file's temporary, and
    // no concurrent writer, shares it.
    let temp = temporary(&path);
    let body = toml::to_string(sidecar)
        .map_err(|e| StorageError::Database(format!("encoding the store's sidecar: {e}")))?;
    {
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, &path)?;
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn temporary(sidecar: &Path) -> PathBuf {
    let name = sidecar.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    sidecar.with_file_name(format!("{name}.tmp.{}", std::process::id()))
}

/// Remove the temporary files a writer that is gone left behind. A temporary
/// whose process is still running is kept: a second start on a live data
/// directory must not delete the live node's file between its create and its
/// rename.
fn remove_stale_temporaries(sidecar: &Path) {
    let (Some(dir), Some(name)) = (sidecar.parent(), sidecar.file_name()) else { return };
    let prefix = format!("{}.tmp.", name.to_string_lossy());
    let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(pid) = file_name.to_string_lossy().strip_prefix(&prefix).map(str::to_owned)
            else {
                continue;
            };
            if !pid.parse().is_ok_and(process_is_running) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

/// Whether a process with this id exists. One that exists but belongs to
/// another user counts: its file is not ours to judge. Public for the
/// lifecycle marker's temporaries, which follow the same rule.
#[cfg(unix)]
pub fn process_is_running(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // SAFETY: signal 0 sends nothing; it only asks whether `pid` exists.
    let rc = unsafe { libc::kill(pid, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Where the question cannot be asked, every writer is taken as running, so
/// a temporary is left rather than deleted under a live writer.
#[cfg(not(unix))]
pub fn process_is_running(_pid: i32) -> bool {
    true
}

/// The sidecar this build writes for a store holding `schema`.
pub(crate) fn stamp(schema: u8, build: &BuildVersions) -> Sidecar {
    Sidecar {
        schema,
        redb_version: format!("{}.{}", build.redb.0, build.redb.1),
        redb_file_format: build.file_format,
        written_by: build.written_by.clone(),
    }
}

/// Read the sidecar, if there is one and it parses.
pub(crate) fn read_sidecar(database: &Path) -> Option<Sidecar> {
    let bytes = std::fs::read(sidecar_path(database)).ok()?;
    parse_sidecar(&sidecar_path(database), &bytes).ok()
}

/// Raise the sidecar's schema to `schema`, never lowering it: `migrate` calls
/// this immediately before the first write of a migration, so an older build
/// refuses a store whose migration has begun (ADR-190).
pub(crate) fn raise_schema(database: &Path, schema: u8, build: &BuildVersions) -> Result<()> {
    let current = read_sidecar(database);
    if current.as_ref().is_some_and(|s| s.schema >= schema) {
        return Ok(());
    }
    info!(schema, path = %sidecar_path(database).display(), "the store's sidecar records the migration");
    write_sidecar(
        database,
        &Sidecar {
            schema,
            redb_version: format!("{}.{}", build.redb.0, build.redb.1),
            redb_file_format: build.file_format,
            written_by: build.written_by.clone(),
        },
    )
}

/// After a successful open and migration: the sidecar agrees with META, never
/// lower, and names this build's redb. Best effort after the open has
/// succeeded: a sidecar that cannot be written is a warning, and the next start
/// reads the store through the fallback.
pub(crate) fn reconcile_after_open(
    database: &Path,
    meta_schema: Option<u8>,
    build: &BuildVersions,
) {
    let current = read_sidecar(database);
    let schema = current.as_ref().map_or(0, |s| s.schema).max(meta_schema.unwrap_or(0));
    let wanted = Sidecar {
        schema,
        redb_version: format!("{}.{}", build.redb.0, build.redb.1),
        redb_file_format: build.file_format,
        written_by: build.written_by.clone(),
    };
    if current.as_ref() == Some(&wanted) {
        return;
    }
    if let Err(e) = write_sidecar(database, &wanted) {
        warn!(error = %e, path = %sidecar_path(database).display(), "could not write the store's sidecar");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;

    fn a_store() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let c = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&c, bson::doc! { "_id": 1 }).unwrap();
        }
        (dir, path)
    }

    /// The store and its sidecar, byte for byte.
    fn snapshot(path: &Path) -> (Vec<u8>, Option<Vec<u8>>) {
        (std::fs::read(path).unwrap(), std::fs::read(sidecar_path(path)).ok())
    }

    fn assert_refused_untouched(path: &Path, build: &BuildVersions, what: &str) {
        let before = snapshot(path);
        match Engine::open_as(path, None, build) {
            Err(StorageError::RefusedStore(why)) => {
                assert!(
                    why.contains("nothing in it was changed") || why.contains("unreadable"),
                    "{what}: {why}"
                )
            }
            Err(other) => panic!("{what}: refused with the wrong error: {other}"),
            Ok(_) => panic!("{what}: opened a store it must refuse"),
        }
        assert!(snapshot(path) == before, "{what}: the store or its sidecar changed");
    }

    fn with_sidecar(path: &Path, edit: impl FnOnce(&mut Sidecar)) {
        let mut sidecar = read_sidecar(path).expect("a sidecar");
        edit(&mut sidecar);
        write_sidecar(path, &sidecar).unwrap();
    }

    /// A copy of a store taken while it is open: a file that needs repair. The
    /// control is that this build does repair it.
    fn a_dirty_copy() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("live.redb");
        let copy = dir.path().join("kimmy.redb");
        let engine = Engine::open(&live).unwrap();
        let c = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&c, bson::doc! { "_id": 1 }).unwrap();
        std::fs::copy(&live, &copy).unwrap();
        std::fs::copy(sidecar_path(&live), sidecar_path(&copy)).unwrap();
        drop(engine);
        (dir, copy)
    }

    fn older() -> BuildVersions {
        BuildVersions { schema: crate::migrate::SCHEMA_VERSION - 1, ..BuildVersions::ours() }
    }

    #[test]
    fn an_open_store_has_a_sidecar_naming_this_build() {
        let (_dir, path) = a_store();
        let sidecar = read_sidecar(&path).expect("written on open");
        assert_eq!(sidecar.schema, crate::migrate::SCHEMA_VERSION);
        assert_eq!(major_minor(&sidecar.redb_version), Some(REDB_MAJOR_MINOR));
        assert_eq!(sidecar.redb_file_format, REDB_FILE_FORMAT);
    }

    #[test]
    fn a_newer_schema_in_the_sidecar_is_refused_untouched_clean_and_dirty() {
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| s.schema += 1);
        assert_refused_untouched(&path, &BuildVersions::ours(), "clean");

        let (_d, dirty) = a_dirty_copy();
        with_sidecar(&dirty, |s| s.schema += 1);
        assert_refused_untouched(&dirty, &BuildVersions::ours(), "dirty");
    }

    #[test]
    fn the_dirty_fixture_is_one_this_build_repairs() {
        let (_d, dirty) = a_dirty_copy();
        let before = std::fs::read(&dirty).unwrap();
        drop(Engine::open(&dirty).unwrap());
        assert_ne!(std::fs::read(&dirty).unwrap(), before, "the open wrote: it repaired");
    }

    #[test]
    fn a_newer_redb_minor_or_file_format_in_the_sidecar_is_refused_untouched() {
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| {
            s.redb_version = format!("{}.{}", REDB_MAJOR_MINOR.0, REDB_MAJOR_MINOR.1 + 1)
        });
        assert_refused_untouched(&path, &BuildVersions::ours(), "redb minor");
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| s.redb_file_format = REDB_FILE_FORMAT + 1);
        assert_refused_untouched(&path, &BuildVersions::ours(), "file format");
    }

    #[test]
    fn a_newer_redb_patch_alone_is_not_a_boundary() {
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| {
            s.redb_version = format!("{}.{}.99", REDB_MAJOR_MINOR.0, REDB_MAJOR_MINOR.1)
        });
        drop(Engine::open(&path).expect("a patch release is not a boundary"));
    }

    #[test]
    fn a_newer_format_byte_in_the_header_is_refused_before_redb_is_asked() {
        let (_dir, path) = a_store();
        std::fs::remove_file(sidecar_path(&path)).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        for offset in SLOT_OFFSETS {
            bytes[offset] = REDB_FILE_FORMAT + 1;
        }
        std::fs::write(&path, &bytes).unwrap();
        assert_refused_untouched(&path, &BuildVersions::ours(), "header");
    }

    /// With no sidecar, the read-only open reads META: a newer schema, or a
    /// newer redb recorded there, is refused on a clean store.
    #[test]
    fn with_no_sidecar_a_newer_schema_or_redb_in_meta_is_refused_untouched() {
        for (key, value) in [
            (tables::META_FORMAT_VERSION, vec![crate::migrate::SCHEMA_VERSION + 1]),
            (
                META_REDB_VERSION,
                format!("{}.{}", REDB_MAJOR_MINOR.0, REDB_MAJOR_MINOR.1 + 1).into_bytes(),
            ),
        ] {
            let (_dir, path) = a_store();
            std::fs::remove_file(sidecar_path(&path)).unwrap();
            {
                let db = redb::Database::create(&path).unwrap();
                let txn = db.begin_write().unwrap();
                txn.open_table(tables::META).unwrap().insert(key, value.as_slice()).unwrap();
                txn.commit().unwrap();
            }
            assert_refused_untouched(&path, &BuildVersions::ours(), key);
        }
    }

    #[test]
    fn an_unreadable_sidecar_is_refused_untouched_and_an_unknown_field_is_not() {
        for (what, body) in [
            ("unparseable", "schema = ".to_string()),
            ("a missing field", "schema = 4\nredb_version = \"4.1\"\n".to_string()),
            ("a wrong type", "schema = \"four\"\nredb_version = \"4.1\"\nredb_file_format = 3\nwritten_by = \"x\"\n".to_string()),
            ("a bad version", "schema = 4\nredb_version = \"four\"\nredb_file_format = 3\nwritten_by = \"x\"\n".to_string()),
        ] {
            let (_dir, path) = a_store();
            std::fs::write(sidecar_path(&path), body).unwrap();
            assert_refused_untouched(&path, &BuildVersions::ours(), what);
        }
        let (_dir, path) = a_store();
        let mut body = std::fs::read_to_string(sidecar_path(&path)).unwrap();
        body.push_str("from_a_later_build = true\n");
        std::fs::write(sidecar_path(&path), body).unwrap();
        drop(
            Engine::open(&path)
                .expect("an unknown field is a later build's, not a reason to refuse"),
        );
    }

    #[test]
    fn a_dirty_store_with_no_sidecar_is_repaired_and_gains_one() {
        let (_d, dirty) = a_dirty_copy();
        std::fs::remove_file(sidecar_path(&dirty)).unwrap();
        drop(Engine::open(&dirty).expect("a store from before the sidecar opens as before"));
        assert_eq!(read_sidecar(&dirty).map(|s| s.schema), Some(crate::migrate::SCHEMA_VERSION));
    }

    #[test]
    fn an_older_build_refuses_this_builds_store_untouched() {
        let (_dir, path) = a_store();
        assert_refused_untouched(&path, &older(), "the rollback");
    }

    #[test]
    fn a_zero_length_file_is_fresh_and_a_short_one_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        std::fs::write(&path, b"").unwrap();
        drop(Engine::open(&path).expect("an empty file is a fresh store"));

        let short = dir.path().join("short.redb");
        std::fs::write(&short, vec![0u8; 100]).unwrap();
        assert_refused_untouched(&short, &BuildVersions::ours(), "short");
    }

    #[test]
    fn a_sidecar_with_no_database_is_overwritten_and_a_stale_temporary_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        std::fs::write(
            sidecar_path(&path),
            "schema = 99\nredb_version = \"99.0\"\nredb_file_format = 99\nwritten_by = \"x\"\n",
        )
        .unwrap();
        // No process has this id, so its writer is gone.
        let stale = dir.path().join(format!("kimmy.format.tmp.{}", i32::MAX));
        std::fs::write(&stale, "garbage").unwrap();
        drop(Engine::open(&path).expect("nothing to protect"));
        assert_eq!(read_sidecar(&path).map(|s| s.schema), Some(crate::migrate::SCHEMA_VERSION));
        assert!(!stale.exists(), "a stale temporary is removed");
    }

    /// A second start on a live data directory keeps the live writer's
    /// temporary: deleting it between its create and its rename would fail the
    /// live node's sidecar write.
    #[test]
    fn a_temporary_whose_writer_is_running_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let parent = std::os::unix::process::parent_id();
        let live = dir.path().join(format!("kimmy.format.tmp.{parent}"));
        std::fs::write(&live, "in flight").unwrap();
        remove_stale_temporaries(&sidecar_path(&path));
        assert!(live.exists(), "the running writer's temporary is kept");
    }

    /// A store another process holds is refused by the read before the lock,
    /// and nothing is written, the sidecar included.
    #[test]
    fn a_store_another_holder_has_open_is_refused_and_its_sidecar_untouched() {
        let (_dir, path) = a_store();
        let holder = Engine::open(&path).unwrap();
        let sidecar = std::fs::read(sidecar_path(&path)).unwrap();
        assert!(matches!(
            Engine::open_as(&path, None, &older()),
            Err(StorageError::RefusedStore(_)) | Err(StorageError::UnsupportedFormat { .. })
        ));
        assert!(
            matches!(Engine::open(&path), Err(StorageError::StoreInUse(_))),
            "and this build too, while it is held"
        );
        assert_eq!(std::fs::read(sidecar_path(&path)).unwrap(), sidecar);
        // With no sidecar, the read-only fallback is what meets the holder.
        std::fs::remove_file(sidecar_path(&path)).unwrap();
        assert!(matches!(Engine::open(&path), Err(StorageError::StoreInUse(_))));
        assert!(!sidecar_path(&path).exists(), "no sidecar is written for a held store");
        drop(holder);
    }

    #[test]
    fn the_redb_constants_are_what_this_build_is_on() {
        let lock =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"))
                .unwrap();
        let version = lock
            .split("[[package]]")
            .find(|p| p.contains("name = \"redb\"\n"))
            .and_then(|p| p.lines().find_map(|l| l.strip_prefix("version = \"")))
            .map(|v| v.trim_end_matches('"').to_string())
            .expect("redb in Cargo.lock");
        assert_eq!(major_minor(&version), Some(REDB_MAJOR_MINOR), "Cargo.lock has redb {version}");

        let (_dir, path) = a_store();
        let header = read_header(&mut std::fs::File::open(&path).unwrap()).unwrap();
        assert!(
            SLOT_OFFSETS.iter().any(|&o| header[o] == REDB_FILE_FORMAT),
            "a store this build writes carries file format {REDB_FILE_FORMAT}"
        );
    }

    /// A backup is logical, so a restore regenerates the sidecar from the
    /// restoring build, and a build on an older redb opens what it restored.
    #[test]
    fn a_restore_regenerates_the_sidecar_for_the_build_that_restores() {
        let (_dir, path) = a_store();
        let engine = Engine::open(&path).unwrap();
        let mut backup = Vec::new();
        engine.backup_to(&mut backup).unwrap();
        drop(engine);

        let older_redb = BuildVersions {
            redb: (REDB_MAJOR_MINOR.0, REDB_MAJOR_MINOR.1 - 1),
            ..BuildVersions::ours()
        };
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("kimmy.redb");
        crate::backup::restore_with(&restored, &mut backup.as_slice(), &older_redb).unwrap();
        let sidecar = read_sidecar(&restored).expect("regenerated");
        assert_eq!(major_minor(&sidecar.redb_version), Some(older_redb.redb));
        drop(Engine::open_as(&restored, None, &older_redb).expect("the restoring build opens it"));
    }
}
