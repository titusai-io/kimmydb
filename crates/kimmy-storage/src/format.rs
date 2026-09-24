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
/// to `~4.3`, so a patch release is the only kind that can arrive unnoticed; a
/// test holds this constant to `Cargo.lock`.
pub const REDB_MAJOR_MINOR: (u64, u64) = (4, 3);

/// The file-format byte redb 4.3 writes in each commit slot, the same as 4.1's.
/// A test creates a store and reads it back.
pub const REDB_FILE_FORMAT: u8 = 3;

/// META key holding the redb major.minor that last opened the store for
/// writing, so the read-only fallback catches a newer redb on a clean store
/// with no sidecar.
pub(crate) const META_REDB_VERSION: &str = "redb_version";

const HEADER_LEN: usize = 320;
const MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const SLOT_OFFSETS: [usize; 2] = [64, 192];
/// The god byte, whose bit 0 names the primary slot, and the region geometry
/// after it (redb 4.3, `header.rs:13-63`).
const GOD_BYTE: usize = MAGIC.len();
const PRIMARY_BIT: u8 = 1;
const TWO_PHASE_COMMIT: u8 = 4;
const PAGE_SIZE_OFFSET: usize = 12;
const REGION_HEADER_PAGES_OFFSET: usize = 16;
const REGION_MAX_DATA_PAGES_OFFSET: usize = 20;
/// A commit slot's two B-tree roots, as (non-null flag, page number) offsets
/// within the slot: the user root, then the system root (`header.rs:65-73`).
/// The slot's other page number, formerly the freed tree's root, is unused and
/// never read.
const SLOT_ROOTS: [(usize, usize); 2] = [(1, 8), (2, 40)];
/// How the way out of a damaged store is put, in every refusal of one.
const DAMAGED_WAY_OUT: &str = "Restore it from a backup, or on a cluster member wipe the data \
     directory and let it catch up from its peers (see operations.md, \"A damaged store\")";

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
    /// The database's length when it was checked.
    len: u64,
    sidecar: Option<Vec<u8>>,
    /// The sidecar's modification time, so a sidecar put back is as it was.
    sidecar_modified: Option<std::time::SystemTime>,
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
            len,
            sidecar: None,
            sidecar_modified: None,
            build: build.clone(),
        });
    }
    if (len as usize) < HEADER_LEN {
        return Err(refused(database, format!("the file is {len} bytes, shorter than a header")));
    }
    let header = read_header(&mut std::fs::File::open(database)?)?;
    check_header(database, &header, build)?;
    check_roots(database, &header, len)?;

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
    let sidecar_modified = match sidecar_bytes {
        Some(_) => std::fs::metadata(&sidecar_file).and_then(|m| m.modified()).ok(),
        None => None,
    };
    Ok(Cleared {
        prior,
        header: Some(header),
        len,
        sidecar: sidecar_bytes,
        sidecar_modified,
        build: build.clone(),
    })
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

/// The byte ranges of the roots in the slot at `slot`, with each page's order,
/// computed as redb 4.3 computes them, or `None` for a file format whose slot
/// layout this build does not know.
///
/// redb 4.3 decodes a page number in `PageNumber::from_le_bytes`
/// (`base.rs:97-108`): the order is the top five bits, the region the 20 bits
/// above bit 20, and the index the low bits under a mask shrunk by the order.
/// `TransactionalMemory::get_page` (`page_manager.rs:1905-1913`) reads it at
/// `PageNumber::address_range` (`base.rs:142-161`), called with the page size
/// as the data section's offset, the full region's length, and its header's
/// length (`page_manager.rs:1216-1217`, `layout.rs:48-72`). The sums are in
/// `u128`, so a damaged header cannot overflow them.
fn root_ranges(header: &[u8], slot: usize) -> Option<Vec<(std::ops::Range<u128>, u32)>> {
    if SLOT_OFFSETS.iter().any(|&o| header[o] != REDB_FILE_FORMAT) {
        return None;
    }
    let u32_at =
        |o: usize| u128::from(u32::from_le_bytes(header[o..o + 4].try_into().expect("4 bytes")));
    let page_size = u32_at(PAGE_SIZE_OFFSET);
    let region_header = u32_at(REGION_HEADER_PAGES_OFFSET) * page_size;
    let region_len = region_header + u32_at(REGION_MAX_DATA_PAGES_OFFSET) * page_size;
    let mut ranges = Vec::new();
    for (non_null, at) in SLOT_ROOTS {
        if header[slot + non_null] == 0 {
            continue;
        }
        let raw = u64::from_le_bytes(header[slot + at..slot + at + 8].try_into().expect("8 bytes"));
        let order = (raw >> 59) as u32;
        let index = u128::from(raw & (0x000F_FFFF >> order));
        let region = u128::from((raw >> 20) & 0x000F_FFFF);
        let page_bytes = (1u128 << order) * page_size;
        let start = page_size + region * region_len + region_header + index * page_bytes;
        ranges.push((start..start + page_bytes, order));
    }
    Some(ranges)
}

/// The first root in the slot at `slot` that ends past `len`.
fn root_past_end(header: &[u8], slot: usize, len: u64) -> Option<(std::ops::Range<u128>, u32)> {
    root_ranges(header, slot)?.into_iter().find(|(range, _)| range.end > u128::from(len))
}

/// Refuse a store whose commit slot names a root page that ends past the
/// file, where redb would read it.
///
/// **This exists because of redb 4.3.0, and is to be removed with it.** redb
/// 4.3 refuses a root of order above 20 before reading it, but one of order 20
/// or less it reads into a zero-filled buffer of the page's length first, up to
/// 4 GiB, and only then fails at the file's end. Under a 2 GiB memory limit
/// that start is OOM-killed rather than refused. Reported upstream at
/// <ISSUE-URL>. Once kimmydb depends on a redb that refuses such a page
/// without allocating it, delete this check, `root_ranges` and their tests:
/// `format::tests::redb_itself_still_allocates_for_a_root_page_past_eof`
/// fails on the first redb that does.
///
/// Which slot redb reads decides what is checked, as redb 4.3 decides it
/// (`header.rs`, `select_primary_slot`). With the two-phase bit, which every
/// clean close sets, redb reads the primary slot or refuses the store, so the
/// primary's roots must fit. Without it, redb picks between the slots by their
/// checksums, which this does not compute; the store is refused only when
/// both slots name a root past the end, since then whichever redb picks would
/// allocate. A torn primary slot on a store that crashed is redb's to repair
/// from the secondary.
fn check_roots(database: &Path, header: &[u8], len: u64) -> Result<()> {
    let primary = SLOT_OFFSETS[usize::from(header[GOD_BYTE] & PRIMARY_BIT)];
    let Some((range, order)) = root_past_end(header, primary, len) else { return Ok(()) };
    let two_phase = header[GOD_BYTE] & TWO_PHASE_COMMIT != 0;
    let secondary = SLOT_OFFSETS[usize::from(header[GOD_BYTE] & PRIMARY_BIT) ^ 1];
    if !two_phase && root_past_end(header, secondary, len).is_none() {
        return Ok(());
    }
    let which = if two_phase { "primary commit slot" } else { "commit slots both" };
    Err(refused(
        database,
        format!(
            "its {which} name a root page past the file's end (order {order}, bytes \
             {}..{}, file {len} bytes), so it is damaged. {DAMAGED_WAY_OUT}",
            range.start, range.end
        ),
    ))
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
pub(crate) fn major_minor(version: &str) -> Option<(u64, u64)> {
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
            header_refusal(&e).unwrap_or_else(|| {
                format!("it could not be read before opening it for writing: {e}")
            }),
        )),
    }
}

/// Why redb refused a store as damaged (`Corrupted`), or as an older file
/// format it would upgrade by writing (`UpgradeRequired`).
///
/// In redb 4.3 the header's refusals come from
/// `TransactionalMemory::read_header`, which runs after the open's lock and
/// before anything is written: `Corrupted` from
/// `UnrepairedDatabaseHeader::from_bytes` (page size, region counts, the slot
/// format byte) and `finalize` (the file length against the layout, the
/// primary slot's checksum), `UpgradeRequired` from
/// `TransactionHeader::from_bytes`. The header is written back only after
/// `finalize` returns. On a clean store, the page-order check that refuses a
/// slot naming too large a root page (`TransactionalMemory::get_page`) runs in
/// the first tree read, before `begin_writable` writes. A `Corrupted` from a
/// tree walk during a repair can follow a write, which is why
/// [`Cleared::after_failed_open`] also looks at the file.
fn header_refusal(error: &redb::DatabaseError) -> Option<String> {
    match error {
        redb::DatabaseError::Storage(redb::StorageError::Corrupted(why)) => {
            Some(format!("redb refused it as damaged ({why}). {DAMAGED_WAY_OUT}"))
        }
        redb::DatabaseError::UpgradeRequired(format) => Some(format!(
            "it is in redb file format {format}, which redb would upgrade by writing to it. \
             Start the build that wrote it, or restore a backup (see operations.md, \"Rolling \
             back, and kimmy.format\")"
        )),
        _ => None,
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
    /// What a failed read-write open returns, and whether the sidecar written
    /// before it is put back.
    ///
    /// The sidecar goes on disk before redb's open on purpose: if redb writes
    /// to the store and the process dies, an older build must refuse it. So it
    /// is put back only when redb wrote nothing: a refusal at the header
    /// ([`header_refusal`]), with the header and the length still what the
    /// check read. Any other failure keeps it.
    pub(crate) fn after_failed_open(
        &self,
        database: &Path,
        error: redb::DatabaseError,
        sidecar_written: bool,
    ) -> StorageError {
        let Some(why) = header_refusal(&error) else { return error.into() };
        if !self.store_unchanged(database) {
            return error.into();
        }
        if sidecar_written {
            let path = sidecar_path(database);
            let put_back = match &self.sidecar {
                Some(bytes) => write_sidecar_bytes(database, bytes, self.sidecar_modified),
                None => std::fs::remove_file(&path).map_err(Into::into),
            };
            if let Err(e) = put_back {
                return StorageError::RefusedStore(format!(
                    "{} is not opened by this build: {why}. Nothing in it was written, but the \
                     sidecar written before redb's open could not be put back ({e}); restore \
                     {} from the backup",
                    database.display(),
                    path.display()
                ));
            }
        }
        refused(database, why)
    }

    /// Whether the database's header and length are still what the check read.
    fn store_unchanged(&self, database: &Path) -> bool {
        let Some(checked) = &self.header else { return false };
        let Ok(mut file) = std::fs::File::open(database) else { return false };
        let len = file.metadata().map(|m| m.len()).ok();
        len == Some(self.len) && read_header(&mut file).ok().as_ref() == Some(checked)
    }

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
        // The same header, against the file's length now.
        if let Some(header) = &header {
            check_roots(database, header, len)?;
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
    let body = toml::to_string(sidecar)
        .map_err(|e| StorageError::Database(format!("encoding the store's sidecar: {e}")))?;
    write_sidecar_bytes(database, body.as_bytes(), None)
}

/// [`write_sidecar`], of bytes already encoded: the same temporary, sync and
/// rename, and the modification time `modified` if one is given.
fn write_sidecar_bytes(
    database: &Path,
    body: &[u8],
    modified: Option<std::time::SystemTime>,
) -> Result<()> {
    use std::io::Write;
    let path = sidecar_path(database);
    // Named for the sidecar and this process: no other file's temporary, and
    // no concurrent writer, shares it.
    let temp = temporary(&path);
    {
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(body)?;
        if let Some(modified) = modified {
            file.set_modified(modified)?;
        }
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
///
/// In a container, every run is pid 1, so a temporary an earlier run left as
/// `.tmp.1` reads as this process's and is kept. That is harmless: nothing
/// reads a temporary, and this process's own next write uses the same name,
/// truncating it and renaming it over the sidecar.
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
        assert!(
            matches!(
                redb::Builder::new().open_read_only(&dirty),
                Err(redb::DatabaseError::RepairAborted)
            ),
            "the fixture is a file redb must repair"
        );
        let before = std::fs::read(&dirty).unwrap();
        drop(Engine::open(&dirty).unwrap());
        assert_ne!(std::fs::read(&dirty).unwrap(), before, "the open wrote: it repaired");
        assert!(redb::Builder::new().open_read_only(&dirty).is_ok(), "and it is clean after");
    }

    /// A dirty store with no sidecar cannot be read before redb repairs it, so
    /// the check lets it through (the gap ADR-190 documents). If META then
    /// says a newer redb wrote it, the open stops before the ensure-tables
    /// commit, and META keeps saying so: the next start refuses it read-only.
    #[test]
    fn a_dirty_store_a_newer_redb_wrote_with_no_sidecar_stops_and_keeps_the_evidence() {
        let (dir, live) = a_store();
        std::fs::remove_file(sidecar_path(&live)).unwrap();
        let newer = format!("{}.{}", REDB_MAJOR_MINOR.0, REDB_MAJOR_MINOR.1 + 1);
        let copy = dir.path().join("copy.redb");
        {
            let db = redb::Database::create(&live).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::META)
                .unwrap()
                .insert(META_REDB_VERSION, newer.as_bytes())
                .unwrap();
            txn.commit().unwrap();
            std::fs::copy(&live, &copy).unwrap();
        }
        assert!(matches!(
            redb::Builder::new().open_read_only(&copy),
            Err(redb::DatabaseError::RepairAborted)
        ));
        match Engine::open(&copy) {
            Err(StorageError::RefusedStore(why)) => assert!(why.contains(&newer), "{why}"),
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a store a newer redb wrote"),
        }
        assert!(!sidecar_path(&copy).exists(), "no sidecar claims this build wrote it");
        {
            let db = redb::Builder::new().open_read_only(&copy).unwrap();
            let txn = db.begin_read().unwrap();
            let meta = txn.open_table(tables::META).unwrap();
            let recorded = meta.get(META_REDB_VERSION).unwrap().unwrap().value().to_vec();
            assert_eq!(recorded, newer.as_bytes(), "META still names the newer redb");
        }
        assert_refused_untouched(&copy, &BuildVersions::ours(), "the next start");
    }

    #[test]
    fn an_open_records_its_redb_in_meta() {
        let (_dir, path) = a_store();
        let db = redb::Builder::new().open_read_only(&path).unwrap();
        let txn = db.begin_read().unwrap();
        let meta = txn.open_table(tables::META).unwrap();
        let recorded = meta.get(META_REDB_VERSION).unwrap().expect("recorded on open");
        let recorded = std::str::from_utf8(recorded.value()).unwrap().to_string();
        assert_eq!(major_minor(&recorded), Some(REDB_MAJOR_MINOR));
    }

    /// With no sidecar, any read-only error other than "needs repair" refuses:
    /// here, an older file format that redb would upgrade by writing.
    #[test]
    fn with_no_sidecar_a_store_redb_would_upgrade_is_refused_untouched() {
        let (_dir, path) = a_store();
        std::fs::remove_file(sidecar_path(&path)).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        for offset in SLOT_OFFSETS {
            bytes[offset] = REDB_FILE_FORMAT - 1;
        }
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            redb::Builder::new().open_read_only(&path),
            Err(redb::DatabaseError::UpgradeRequired(_))
        ));
        assert_refused_untouched(&path, &BuildVersions::ours(), "an older format byte");
    }

    /// What the check read is read again under the lock: a sidecar or header
    /// that changed in between refuses the open, and nothing is written.
    #[test]
    fn a_store_that_changes_after_the_check_is_refused_under_the_lock() {
        for what in ["sidecar", "header"] {
            let (_dir, path) = a_store();
            let cleared = check_before_open(&path).unwrap();
            if what == "sidecar" {
                with_sidecar(&path, |s| s.schema = crate::migrate::SCHEMA_VERSION + 1);
            } else {
                let mut bytes = std::fs::read(&path).unwrap();
                for offset in SLOT_OFFSETS {
                    bytes[offset] = REDB_FILE_FORMAT + 1;
                }
                std::fs::write(&path, &bytes).unwrap();
            }
            let before = snapshot(&path);
            match Engine::open_cleared(&path, None, cleared) {
                Err(StorageError::RefusedStore(why)) => {
                    assert!(why.contains("changed while it was being checked"), "{what}: {why}")
                }
                Err(other) => panic!("{what}: refused with the wrong error: {other}"),
                Ok(_) => panic!("{what}: opened a store that changed under the check"),
            }
            assert!(snapshot(&path) == before, "{what}: nothing is written");
        }
    }

    /// A backup of a newer schema is refused before the restore creates a file.
    #[test]
    fn a_restore_refuses_a_backup_of_a_newer_schema_before_writing() {
        let (_dir, path) = a_store();
        let engine = Engine::open(&path).unwrap();
        let mut backup = Vec::new();
        engine.backup_to(&mut backup).unwrap();
        drop(engine);
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("kimmy.redb");
        match crate::backup::restore_with(&restored, &mut backup.as_slice(), &older()) {
            Err(StorageError::UnsupportedFormat { found, expected }) => {
                assert_eq!((found, expected), (crate::migrate::SCHEMA_VERSION, older().schema))
            }
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("restored a newer schema"),
        }
        assert!(!restored.exists(), "no file is created");
        assert!(!sidecar_path(&restored).exists());
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

    // A damaged header, and the store's lock (redb 4.3).

    /// The damage the `redb_damage` example does to a real store, so a test
    /// round's fixture and these cannot diverge.
    mod damage {
        #![allow(dead_code)]
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/redb_damage/damage.rs"));
    }
    use damage::{GOD_BYTE, PRIMARY_BIT, RECOVERY_REQUIRED, TWO_PHASE_COMMIT};

    /// What redb 4.3 answers a clean store whose primary slot fails its
    /// checksum: a clean close commits with two-phase commit, and such a
    /// primary is never passed over for the secondary.
    const PRIMARY_CORRUPTED: &str = "Primary is corrupted despite 2-phase commit";
    /// What this build answers a primary slot naming a root page past the
    /// file's end, before redb reads it ([`check_roots`]).
    const ROOT_PAST_END: &str = "past the file's end";
    /// The most a refused open may make the child's resident set: an open
    /// that allocates the page it was pointed at blows through it.
    const REFUSAL_MAX_RSS_KIB: u64 = 256 * 1024;
    /// How long a child is given to open a store before it is killed. The
    /// open itself must answer within a second; the rest is a debug test
    /// binary starting.
    const CHILD_LIMIT: std::time::Duration = std::time::Duration::from_secs(15);
    const PROBE_OPEN: &str = "KIMMY_FORMAT_PROBE_OPEN";
    const PROBE_HOLD: &str = "KIMMY_FORMAT_PROBE_HOLD";

    fn edit_store(path: &Path, edit: impl FnOnce(&mut Vec<u8>)) {
        let mut bytes = std::fs::read(path).unwrap();
        edit(&mut bytes);
        std::fs::write(path, &bytes).unwrap();
    }

    /// Both commit slots' bytes 1..128 set to 0xFF, the format byte left at 3:
    /// the store that made redb 4.1 allocate 8 TiB and fill it.
    fn damage_both_slots(path: &Path) {
        edit_store(path, |b| {
            for slot in SLOT_OFFSETS {
                b[slot + 1..slot + 128].fill(0xFF);
            }
        });
    }

    /// One byte: the primary slot's system-root page order set to `order`, a
    /// page of 2^order pages. The slot's checksum no longer matches.
    fn damage_page_order(path: &Path, order: u8) {
        edit_store(path, |b| {
            damage::set_primary_page_order(b, order, false).unwrap();
        });
    }

    /// The same byte, with the slot's checksum recomputed: only a check of the
    /// page order itself can refuse it.
    fn damage_page_order_validly(path: &Path, order: u8) {
        edit_store(path, |b| {
            let change = damage::set_primary_page_order(b, order, true).unwrap();
            assert!(damage::slot_checksum_valid(b, change.offset));
        });
    }

    /// A clean store's close commits with two-phase commit, which is what makes
    /// redb refuse a damaged primary rather than open the secondary. A redb
    /// that stops doing so fails here, not in a refusal that went missing.
    fn assert_two_phase(path: &Path, what: &str) {
        let god = std::fs::read(path).unwrap()[GOD_BYTE];
        assert!(god & TWO_PHASE_COMMIT != 0, "{what}: god byte {god:#04x} has no two-phase bit");
    }

    fn damage_secondary_slot(path: &Path) {
        edit_store(path, |b| {
            let secondary = SLOT_OFFSETS[usize::from(b[GOD_BYTE] & PRIMARY_BIT) ^ 1];
            b[secondary + 1..secondary + 128].fill(0xFF);
        });
    }

    /// A file's digest, length and modification time.
    #[derive(Debug, PartialEq, Eq)]
    struct Fingerprint {
        sha256: Vec<u8>,
        len: u64,
        modified: std::time::SystemTime,
    }

    fn fingerprint(path: &Path) -> Option<Fingerprint> {
        use sha2::Digest;
        let bytes = std::fs::read(path).ok()?;
        let meta = std::fs::metadata(path).ok()?;
        Some(Fingerprint {
            sha256: sha2::Sha256::digest(&bytes).to_vec(),
            len: meta.len(),
            modified: meta.modified().unwrap(),
        })
    }

    /// The store's and its sidecar's.
    fn fingerprints(path: &Path) -> (Option<Fingerprint>, Option<Fingerprint>) {
        (fingerprint(path), fingerprint(&sidecar_path(path)))
    }

    /// The sidecars a damaged store is tried with: none; this build's, which
    /// the open does not rewrite; and one an older redb stamped, which it
    /// rewrites before redb's open and must put back.
    #[derive(Clone, Copy, Debug)]
    enum SidecarCase {
        None,
        Ours,
        OlderRedb,
    }

    const SIDECAR_CASES: [SidecarCase; 3] =
        [SidecarCase::None, SidecarCase::Ours, SidecarCase::OlderRedb];

    fn with_sidecar_case(path: &Path, case: SidecarCase) {
        match case {
            SidecarCase::None => std::fs::remove_file(sidecar_path(path)).unwrap(),
            SidecarCase::Ours => {}
            SidecarCase::OlderRedb => with_sidecar(path, |s| s.redb_version = "4.1".into()),
        }
    }

    struct ChildOpen {
        elapsed_ms: u64,
        max_rss_kib: u64,
        outcome: String,
    }

    /// Run this test binary again, as a child running [`child_probe`] with
    /// `var` set to `path`, and return what it printed. A child still running
    /// after `limit` is killed, and the test fails: a regression to the hang
    /// neither hangs the suite nor keeps allocating.
    fn run_child(var: &str, path: &Path, limit: std::time::Duration) -> String {
        use std::io::Read as _;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "format::tests::child_probe", "--nocapture", "--test-threads=1"])
            .env(var, path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + limit;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "opening {} did not finish within {limit:?}, so it was killed",
                    path.display()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        let (mut out, mut err) = (String::new(), String::new());
        child.stdout.take().unwrap().read_to_string(&mut out).unwrap();
        child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
        match out.lines().find_map(|l| l.split_once("PROBE ").map(|(_, rest)| rest)) {
            Some(line) => line.to_string(),
            None => panic!(
                "the child opening {} ended {status} without a result\nstdout: {out}\nstderr: {err}",
                path.display()
            ),
        }
    }

    fn open_in_child(path: &Path) -> ChildOpen {
        let line = run_child(PROBE_OPEN, path, CHILD_LIMIT);
        let mut parts = line.splitn(3, ' ');
        let mut number = || parts.next().unwrap().parse().unwrap();
        let (elapsed_ms, max_rss_kib) = (number(), number());
        ChildOpen { elapsed_ms, max_rss_kib, outcome: parts.next().unwrap().to_string() }
    }

    /// This process's peak resident set, in KiB.
    fn max_rss_kib() -> u64 {
        // SAFETY: `getrusage` fills the struct it is given and reads nothing.
        let usage = unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &mut usage);
            usage
        };
        let max = usage.ru_maxrss as u64;
        if cfg!(target_vendor = "apple") { max / 1024 } else { max }
    }

    /// The child side of [`run_child`]. Does nothing unless one of its
    /// variables is set.
    #[test]
    fn child_probe() {
        use std::io::Write as _;
        if let Ok(path) = std::env::var(PROBE_OPEN) {
            let started = std::time::Instant::now();
            let outcome = match Engine::open(Path::new(&path)) {
                Ok(engine) => {
                    let found = engine
                        .get_collection("shop", "orders")
                        .ok()
                        .and_then(|c| engine.get(&c, &kimmy_core::DocId::String("a".into())).ok())
                        .flatten()
                        .is_some();
                    format!("opened found={found}")
                }
                Err(StorageError::RefusedStore(why)) => format!("refused {why}"),
                Err(StorageError::StoreInUse(why)) => format!("in_use {why}"),
                Err(other) => format!("error {other}"),
            };
            // On a line of its own: libtest has already printed the test's name.
            println!("\nPROBE {} {} {outcome}", started.elapsed().as_millis(), max_rss_kib());
        } else if let Ok(path) = std::env::var(PROBE_HOLD) {
            let engine = Engine::open(Path::new(&path)).unwrap();
            let c = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&c, bson::doc! { "_id": "a" }).unwrap();
            println!("\nPROBE ready");
            std::io::stdout().flush().unwrap();
            std::thread::sleep(std::time::Duration::from_secs(120));
            drop(engine);
        }
    }

    /// Opened in a child, refused as damaged for `reason` within a second, and
    /// the store and its sidecar unchanged: digest, length and modification
    /// time.
    fn assert_refused_as_damaged_untouched(path: &Path, what: &str, reason: &str) {
        let before = fingerprints(path);
        let opened = open_in_child(path);
        // The bounds first: an open that allocated the page and then failed
        // some other way is the regression these tests exist for.
        assert!(
            opened.max_rss_kib < REFUSAL_MAX_RSS_KIB,
            "{what}: the open peaked at {} KiB resident: {}",
            opened.max_rss_kib,
            opened.outcome
        );
        assert!(opened.elapsed_ms < 1_000, "{what}: answered after {} ms", opened.elapsed_ms);
        assert!(opened.outcome.starts_with("refused "), "{what}: {}", opened.outcome);
        assert!(
            opened.outcome.contains("nothing in it was changed")
                && opened.outcome.contains("damaged")
                && opened.outcome.contains(reason)
                && opened.outcome.contains(&path.display().to_string()),
            "{what}: {}",
            opened.outcome
        );
        assert!(fingerprints(path) == before, "{what}: the store or its sidecar changed");
    }

    #[test]
    fn a_store_with_both_commit_slots_damaged_is_refused_within_a_second_untouched() {
        for case in SIDECAR_CASES {
            let (_dir, path) = a_store();
            with_sidecar_case(&path, case);
            damage_both_slots(&path);
            let what = format!("both slots, sidecar {case:?}");
            assert_two_phase(&path, &what);
            assert_refused_as_damaged_untouched(&path, &what, ROOT_PAST_END);
        }
    }

    #[test]
    fn one_byte_of_page_order_in_the_primary_slot_is_refused_within_a_second_untouched() {
        for order in [31, 24] {
            for case in SIDECAR_CASES {
                let (_dir, path) = a_store();
                with_sidecar_case(&path, case);
                damage_page_order(&path, order);
                let what = format!("page order {order}, sidecar {case:?}");
                assert_two_phase(&path, &what);
                assert_refused_as_damaged_untouched(&path, &what, reason_for(order));
            }
        }
    }

    /// Which check refuses a primary slot whose root's order was changed and
    /// whose checksum was not: a root that ends past the file is this build's
    /// to refuse, before redb reads anything; one that still fits is redb's,
    /// for the checksum.
    fn reason_for(order: u8) -> &'static str {
        if order >= 2 { ROOT_PAST_END } else { PRIMARY_CORRUPTED }
    }

    /// 4 GiB and 8 GiB pages, which redb 4.1 allocated and then failed to read,
    /// and a page of two, which made it panic.
    #[test]
    fn smaller_page_orders_in_the_primary_slot_are_refused_too() {
        for order in [21, 20, 1] {
            for case in SIDECAR_CASES {
                let (_dir, path) = a_store();
                with_sidecar_case(&path, case);
                damage_page_order(&path, order);
                let what = format!("page order {order}, sidecar {case:?}");
                assert_two_phase(&path, &what);
                assert_refused_as_damaged_untouched(&path, &what, reason_for(order));
            }
        }
    }

    /// A primary slot that verifies, naming a root page past the file's end.
    /// Order 31 is the 8 TiB page redb 4.1 allocated and filled; order 21 is
    /// past the largest redb 4.3 reads. Order 20, 4 GiB, redb 4.3 still
    /// allocates and fills before its read fails at the file's end, which a
    /// start under a smaller memory limit does not survive; only this build's
    /// check stands between the open and that allocation, and the child's peak
    /// resident set is what shows it did.
    #[test]
    fn a_valid_primary_slot_naming_a_root_page_past_the_file_is_refused_untouched() {
        for order in [20, 21, 31] {
            for case in SIDECAR_CASES {
                let (_dir, path) = a_store();
                with_sidecar_case(&path, case);
                damage_page_order_validly(&path, order);
                let what = format!("valid slot, page order {order}, sidecar {case:?}");
                assert_two_phase(&path, &what);
                assert_refused_as_damaged_untouched(&path, &what, ROOT_PAST_END);
            }
        }
    }

    /// [`root_ranges`] mirrors redb's page arithmetic, so on stores redb
    /// wrote, every root it computes lies inside the file, in both slots, and
    /// at the first byte of each primary root is a B-tree page: redb's leaf
    /// (1) or branch (2) type byte. A
    /// redb that lays pages out differently fails this rather than letting the
    /// check refuse good stores. Covered: a small store, one whose table tree's
    /// root is a branch, one whose root leaf is a page of order above 0, and a
    /// dirty store. A store of more than one region needs a file over 4 GiB,
    /// and redb's region size is settable only inside redb's own tests.
    #[test]
    fn every_root_page_of_a_valid_store_lies_inside_it_where_redb_reads_it() {
        let dir = tempfile::tempdir().unwrap();
        let (_small_dir, small) = a_store();
        let (_dirty_dir, dirty) = a_dirty_copy();
        let branch = dir.path().join("branch.redb");
        {
            let db = redb::Database::create(&branch).unwrap();
            let txn = db.begin_write().unwrap();
            for i in 0..2_000 {
                let name = format!("table-{i:05}");
                txn.open_table(redb::TableDefinition::<u64, u64>::new(&name)).unwrap();
            }
            txn.commit().unwrap();
        }
        let big_leaf = dir.path().join("big_leaf.redb");
        {
            let db = redb::Database::create(&big_leaf).unwrap();
            let txn = db.begin_write().unwrap();
            let name = "n".repeat(40_000);
            txn.open_table(redb::TableDefinition::<u64, u64>::new(&name)).unwrap();
            txn.commit().unwrap();
        }
        let mut orders = Vec::new();
        let mut types = Vec::new();
        for path in [&small, &dirty, &branch, &big_leaf] {
            let bytes = std::fs::read(path).unwrap();
            let primary = SLOT_OFFSETS[usize::from(bytes[GOD_BYTE] & PRIMARY_BIT)];
            let secondary = SLOT_OFFSETS[usize::from(bytes[GOD_BYTE] & PRIMARY_BIT) ^ 1];
            for (range, _) in root_ranges(&bytes[..HEADER_LEN], secondary).expect("format 3") {
                assert!(
                    range.end <= bytes.len() as u128,
                    "{}: secondary {range:?}",
                    path.display()
                );
            }
            let ranges = root_ranges(&bytes[..HEADER_LEN], primary).expect("format 3");
            assert!(!ranges.is_empty(), "{}: no root", path.display());
            for (range, order) in ranges {
                assert!(range.end <= bytes.len() as u128, "{}: {range:?}", path.display());
                let first = bytes[range.start as usize];
                assert!(
                    first == 1 || first == 2,
                    "{}: page type {first} at {range:?}",
                    path.display()
                );
                orders.push(order);
                types.push(first);
            }
            drop(Engine::open(path).ok());
        }
        assert!(orders.iter().any(|&o| o > 0), "no root of order above 0: {orders:?}");
        assert!(types.contains(&2), "no branch root: {types:?}");
    }

    /// A store without the two-phase bit, whose two slots both name a root
    /// past the file's end: whichever redb picks, it would allocate the page,
    /// so it is refused.
    #[test]
    fn a_dirty_store_whose_slots_both_name_a_root_past_the_end_is_refused_untouched() {
        let (_d, path) = a_dirty_copy();
        edit_store(&path, |b| {
            for slot in SLOT_OFFSETS {
                // The system root's page order, in the slot's top byte of it.
                b[slot + 47] = (b[slot + 47] & 0x07) | (20 << 3);
            }
        });
        let god = std::fs::read(&path).unwrap()[GOD_BYTE];
        assert!(god & TWO_PHASE_COMMIT == 0, "god byte {god:#04x}");
        assert_refused_as_damaged_untouched(&path, "both slots, dirty", ROOT_PAST_END);
    }

    /// The roots are checked again under the lock, against the file's length
    /// then: a store cut short after the check is refused before redb reads it.
    #[test]
    fn a_store_cut_short_after_the_check_is_refused_under_the_lock() {
        let (_d, path) = a_dirty_copy();
        let cleared = check_before_open(&path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..8192]).unwrap();
        let before = fingerprints(&path);
        match Engine::open_cleared(&path, None, cleared) {
            Err(StorageError::RefusedStore(why)) => assert!(why.contains(ROOT_PAST_END), "{why}"),
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened a store cut short"),
        }
        assert!(fingerprints(&path) == before, "the store or its sidecar changed");
    }

    /// Why [`check_roots`] exists, kept honest: redb 4.3 itself, with no check
    /// in front of it, still reads a root page that lies past the file's end
    /// into a buffer of the page's length before it fails, rather than refusing
    /// it as corrupted. A root of order 12, 16 MiB, keeps this test's own
    /// allocation small; order 20 takes the same path at 4 GiB.
    ///
    /// **When a redb bump makes this fail, because redb now refuses such a page
    /// without reading it, delete `check_roots`, `root_ranges`, this test and
    /// the tests of the check together**, and the paragraph in ADR-190's
    /// addendum that names them.
    #[test]
    fn redb_itself_still_allocates_for_a_root_page_past_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.redb");
        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(redb::TableDefinition::<u64, u64>::new("t"))
                .unwrap()
                .insert(1, 1)
                .unwrap();
            txn.commit().unwrap();
        }
        damage_page_order_validly(&path, 12);
        let len = std::fs::metadata(&path).unwrap().len();
        let header = std::fs::read(&path).unwrap()[..HEADER_LEN].to_vec();
        let primary = SLOT_OFFSETS[usize::from(header[GOD_BYTE] & PRIMARY_BIT)];
        assert!(root_past_end(&header, primary, len).is_some(), "the fixture's root fits");
        let read_past_the_end = |r: std::result::Result<(), redb::DatabaseError>| {
            matches!(
                r,
                Err(redb::DatabaseError::Storage(redb::StorageError::Io(e)))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof
            )
        };
        let read_only = redb::Builder::new().open_read_only(&path).map(drop);
        let read_write = redb::Builder::new().open(&path).map(drop);
        assert!(
            read_past_the_end(read_only) && read_past_the_end(read_write),
            "redb no longer reads a root page past the file's end before refusing it: retire \
             check_roots (see this test's doc comment)"
        );
    }

    /// A store without the two-phase bit, one not shut down cleanly, is
    /// repaired from its secondary slot when its primary fails its checksum.
    #[test]
    fn a_dirty_store_whose_primary_slot_is_damaged_opens_from_the_secondary() {
        let (_d, path) = a_dirty_copy();
        let god = std::fs::read(&path).unwrap()[GOD_BYTE];
        assert!(god & TWO_PHASE_COMMIT == 0 && god & RECOVERY_REQUIRED != 0, "god byte {god:#04x}");
        damage_page_order(&path, 31);
        let opened = open_in_child(&path);
        assert!(opened.outcome.starts_with("opened"), "{}", opened.outcome);
    }

    /// redb reads only the primary slot of a clean store, so damage to the
    /// other one is no reason to refuse it.
    #[test]
    fn a_store_with_only_its_secondary_slot_damaged_still_opens() {
        for case in SIDECAR_CASES {
            let (_dir, path) = a_store();
            with_sidecar_case(&path, case);
            damage_secondary_slot(&path);
            let opened = open_in_child(&path);
            assert!(opened.outcome.starts_with("opened"), "sidecar {case:?}: {}", opened.outcome);
        }
    }

    #[test]
    fn a_clean_store_opens() {
        let (_dir, path) = a_store();
        let opened = open_in_child(&path);
        assert!(opened.outcome.starts_with("opened"), "{}", opened.outcome);
    }

    /// A store whose process was killed with SIGKILL is dirty, is repaired,
    /// and keeps what was committed.
    #[test]
    fn a_store_killed_mid_run_is_repaired_and_opens_with_its_data() {
        use std::io::BufRead as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "format::tests::child_probe", "--nocapture", "--test-threads=1"])
            .env(PROBE_HOLD, &path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let ready = std::io::BufReader::new(holder.stdout.take().unwrap())
            .lines()
            .map_while(|l| l.ok())
            .any(|l| l.ends_with("PROBE ready"));
        holder.kill().unwrap();
        holder.wait().unwrap();
        assert!(ready, "the holder never said it was ready");
        let header = std::fs::read(&path).unwrap();
        assert!(header[GOD_BYTE] & RECOVERY_REQUIRED != 0, "the killed store is dirty");

        let opened = open_in_child(&path);
        assert_eq!(opened.outcome, "opened found=true");
    }

    /// The branch that keeps this build's sidecar: redb's open failed after
    /// it wrote, so the store may already be one an older build must refuse.
    #[test]
    fn an_open_that_fails_after_redb_wrote_keeps_this_builds_sidecar() {
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| s.redb_version = "4.1".into());
        let header = std::fs::read(&path).unwrap()[..HEADER_LEN].to_vec();
        crate::hold_meter::test_hooks::ARM_AT_OPEN.with(|a| a.set(Some("sync_data")));
        let result = Engine::open(&path);
        crate::hold_meter::test_hooks::ARM_AT_OPEN.with(|a| a.set(None));
        match result {
            Err(StorageError::RefusedStore(why)) => {
                panic!("refused, as if nothing was written: {why}")
            }
            Err(_) => {}
            Ok(_) => panic!("the injected fsync failure did not fail the open"),
        }
        assert_ne!(std::fs::read(&path).unwrap()[..HEADER_LEN], header[..], "redb wrote");
        let sidecar = read_sidecar(&path).unwrap();
        assert_eq!(major_minor(&sidecar.redb_version), Some(REDB_MAJOR_MINOR));
    }

    /// A header refusal puts the sidecar back only if the store is as the check
    /// read it; one that changed keeps this build's sidecar.
    #[test]
    fn a_header_refusal_puts_the_sidecar_back_only_if_the_store_is_unchanged() {
        let corrupted =
            || redb::DatabaseError::Storage(redb::StorageError::Corrupted("damaged".into()));
        for (what, error) in
            [("damaged", corrupted()), ("an older format", redb::DatabaseError::UpgradeRequired(2))]
        {
            for changed in [false, true] {
                let (_dir, path) = a_store();
                with_sidecar(&path, |s| s.redb_version = "4.1".into());
                let older = std::fs::read(sidecar_path(&path)).unwrap();
                let cleared = check_before_open(&path).unwrap();
                write_sidecar(&path, &cleared.sidecar_before_open().unwrap()).unwrap();
                if changed {
                    damage_secondary_slot(&path);
                }
                let returned = cleared.after_failed_open(&path, error_clone(&error), true);
                let sidecar = std::fs::read(sidecar_path(&path)).unwrap();
                if changed {
                    assert!(
                        !matches!(returned, StorageError::RefusedStore(_)),
                        "{what}, changed: {returned}"
                    );
                    assert_ne!(
                        sidecar, older,
                        "{what}: a changed store keeps this build's sidecar"
                    );
                } else {
                    assert!(
                        matches!(&returned, StorageError::RefusedStore(why) if why.contains("nothing in it was changed")),
                        "{what}: {returned}"
                    );
                    assert_eq!(sidecar, older, "{what}: the older sidecar is put back");
                }
            }
        }
        // Any other error keeps it, however unchanged the store.
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| s.redb_version = "4.1".into());
        let cleared = check_before_open(&path).unwrap();
        let written = cleared.sidecar_before_open().unwrap();
        write_sidecar(&path, &written).unwrap();
        let returned = cleared.after_failed_open(
            &path,
            redb::DatabaseError::Storage(redb::StorageError::Io(std::io::Error::other("eio"))),
            true,
        );
        assert!(!matches!(returned, StorageError::RefusedStore(_)), "{returned}");
        assert_eq!(read_sidecar(&path), Some(written));
    }

    fn error_clone(error: &redb::DatabaseError) -> redb::DatabaseError {
        match error {
            redb::DatabaseError::UpgradeRequired(v) => redb::DatabaseError::UpgradeRequired(*v),
            redb::DatabaseError::Storage(redb::StorageError::Corrupted(why)) => {
                redb::DatabaseError::Storage(redb::StorageError::Corrupted(why.clone()))
            }
            _ => unreachable!(),
        }
    }

    /// A `flock` held on the store by another descriptor, which is how a
    /// 0.36.x node (redb 4.1) holds it, refuses the open before the sidecar an
    /// older redb stamped is rewritten. On Linux this is the engine's own
    /// `flock`; on macOS, where `flock` and range locks are one namespace, the
    /// range lock alone.
    #[test]
    fn a_flock_held_by_another_descriptor_refuses_the_open_before_anything_is_written() {
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| s.redb_version = "4.1".into());
        let holder = std::fs::File::open(&path).unwrap();
        holder.try_lock().unwrap();
        let before = fingerprints(&path);
        assert!(matches!(Engine::open(&path), Err(StorageError::StoreInUse(_))));
        assert!(fingerprints(&path) == before, "the store or its sidecar changed");
        drop(holder);
        drop(Engine::open(&path).expect("and it opens once the holder lets go"));
    }

    /// The reverse: while an engine holds the store, a 0.36.x node's `flock`
    /// is refused.
    #[test]
    fn while_an_engine_holds_the_store_a_flock_from_another_descriptor_is_refused() {
        let (_dir, path) = a_store();
        let engine = Engine::open(&path).unwrap();
        let other = std::fs::File::open(&path).unwrap();
        assert!(matches!(other.try_lock(), Err(std::fs::TryLockError::WouldBlock)));
        drop(engine);
        other.try_lock().expect("the engine's close releases it");
    }

    /// While an engine holds the store, redb 4.3's own locks are refused: a
    /// whole-storage range lock from another descriptor, and redb's read-write
    /// and read-only opens.
    #[test]
    fn while_an_engine_holds_the_store_redbs_own_locks_are_refused() {
        use redb::StorageBackend as _;
        let (_dir, path) = a_store();
        let engine = Engine::open(&path).unwrap();
        let other = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let backend = redb::backends::FileBackend::new(other).unwrap();
        assert!(matches!(
            backend.try_lock_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            Ok(false)
        ));
        assert!(matches!(
            redb::Builder::new().open(&path),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(matches!(
            redb::Builder::new().open_read_only(&path),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        drop(engine);
        assert!(matches!(
            backend.try_lock_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            Ok(true)
        ));
    }

    /// The lock is held before the check confirms what it read, so a second
    /// engine on a held store is refused before it rewrites a sidecar an
    /// older redb stamped.
    #[test]
    fn a_second_engine_on_a_held_store_is_refused_before_it_writes_the_sidecar() {
        let (_dir, path) = a_store();
        let engine = Engine::open(&path).unwrap();
        with_sidecar(&path, |s| s.redb_version = "4.1".into());
        let sidecar = fingerprint(&sidecar_path(&path));
        assert!(matches!(Engine::open(&path), Err(StorageError::StoreInUse(_))));
        assert!(fingerprint(&sidecar_path(&path)) == sidecar, "the sidecar was rewritten");
        drop(engine);
    }

    /// The lock is held from before the check's confirmation to redb's open:
    /// just before the sidecar is written, another descriptor can take
    /// neither lock.
    #[test]
    fn the_lock_is_held_while_the_sidecar_is_written() {
        use redb::StorageBackend as _;
        let (_dir, path) = a_store();
        with_sidecar(&path, |s| s.redb_version = "4.1".into());
        let seen = std::rc::Rc::new(std::cell::RefCell::new(None));
        let record = std::rc::Rc::clone(&seen);
        crate::store_lock::test_hooks::BEFORE_SIDECAR_WRITE.with(|p| {
            *p.borrow_mut() = Some(Box::new(move |path: &Path| {
                let other = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
                let flock = other.try_lock();
                let backend = redb::backends::FileBackend::new(other).unwrap();
                let range =
                    backend.try_lock_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
                *record.borrow_mut() = Some((
                    matches!(flock, Err(std::fs::TryLockError::WouldBlock)),
                    matches!(range, Ok(false)),
                ));
            }));
        });
        let opened = Engine::open(&path);
        crate::store_lock::test_hooks::BEFORE_SIDECAR_WRITE.with(|p| p.borrow_mut().take());
        drop(opened.unwrap());
        assert_eq!(
            *seen.borrow(),
            Some((true, true)),
            "(flock refused, range lock refused) just before the sidecar write"
        );
    }

    /// What redb 4.3 asks the backend for when it opens: one range of its own,
    /// refused as unsupported, then the whole storage. A redb that asks for
    /// something else has changed how it locks, and fails this before it can
    /// open a store unlocked.
    #[test]
    fn redb_asks_for_the_whole_storage_lock_after_its_own_ranges() {
        use std::ops::Bound;
        let dir = tempfile::tempdir().unwrap();
        crate::store_lock::test_hooks::take();
        drop(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let requests = crate::store_lock::test_hooks::take();
        assert_eq!(requests.len(), 2, "{requests:?}");
        let ((own, _), own_shared) = requests[0];
        assert!(!own_shared && own != Bound::Unbounded, "{requests:?}");
        assert_eq!(requests[1], ((Bound::Unbounded, Bound::Unbounded), false), "{requests:?}");
    }

    /// An open redb made without asking for the lock is refused.
    #[test]
    fn a_store_redb_opened_without_asking_for_the_lock_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        crate::store_lock::test_hooks::PRETEND_NOT_GRANTED.with(|p| p.set(true));
        let result = Engine::open(&dir.path().join("kimmy.redb"));
        crate::store_lock::test_hooks::PRETEND_NOT_GRANTED.with(|p| p.set(false));
        match result {
            Err(StorageError::Database(why)) => assert!(why.contains("without asking"), "{why}"),
            Err(other) => panic!("refused with the wrong error: {other}"),
            Ok(_) => panic!("opened"),
        }
    }
}
