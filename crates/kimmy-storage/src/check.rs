//! `kimmyd check-store`: verify a stopped member's store against redb's page
//! checksums, writing nothing (ADR-204).
//!
//! redb verifies its checksums in its repair after an unclean shutdown and in
//! `Database::check_integrity`, and nowhere else: an ordinary read verifies
//! nothing, so on a store closed cleanly a flipped bit in a page that still
//! decodes is served as data. `check_integrity` walks the whole tree from the
//! primary commit slot, every branch and leaf against its checksum, and
//! rebuilds the allocator state to compare it with the saved one. It is the
//! one public verifier, and it needs the database to itself
//! (`&mut Database`, no live transaction), so it cannot run on a serving
//! handle. That is why this is a command for a stopped member.
//!
//! **redb's check writes, every time.** A clean pass rewrites the header and
//! syncs; a check that finds the primary slot damaged and not two-phase
//! promotes the secondary, which rolls back the latest commit; and the open
//! before it repairs a store that was not closed cleanly, and writes its
//! header either way. So the check runs on a copy-on-write view of the file
//! ([`CowBackend`]): redb reads the file through a read-only descriptor of its
//! own and every write lands in memory, so nothing can reach the file.
//!
//! Around that:
//!
//! - **The store's lock is taken first** ([`crate::store_lock`]), on a
//!   read-write descriptor that is never given to redb and is released only
//!   after redb has closed. A node cannot start on the store while it is
//!   checked, and the check refuses a store a node holds. redb's own lock
//!   requests are answered by the view, which takes no lock: a second
//!   open-file-description lock in this process would conflict with the one
//!   held, and closing a descriptor that carried it would release it.
//! - **The checks before the open** are ADR-190's, without the removal of a
//!   stale temporary sidecar, so nothing in the data directory is written. A
//!   missing or empty store is refused rather than created.
//! - **What the open repaired** is read from redb's repair callback: called
//!   at 0.0 when the store was not closed cleanly, and at 0.3 when the
//!   primary commit slot failed verification and the secondary was promoted,
//!   which discards the latest commit (redb 4.3, `db.rs:1471-1480`). Whether
//!   that is damage depends on how the previous run ended
//!   ([`PreviousRun`]): after a clean stop the store needs no repair at all,
//!   so any repair is damage; after an unclean one, a rolled-back commit is
//!   the torn write redb's repair exists for, and the check cannot tell it
//!   from damage confined to that commit.
//!
//! **What the verdict means.** Only redb's `Corrupted`, a panic in redb, an
//! integrity check that would repair, a repair the previous run's clean end
//! rules out, or a pre-open refusal of a damaged store is damage
//! ([`Verdict::Damaged`], exit 65). An I/O error, a store a newer build wrote,
//! a lock held elsewhere, the view's memory cap, or a stop is
//! [`Verdict::NotChecked`] (exit 1): none of them says anything about the
//! store, and calling one damage would tell an operator to wipe a store that
//! may be healthy.
//!
//! **What it does not check:** `hnsw/`, the vector indexes' snapshots, which
//! are derived, carry no checksum, and are rebuilt when deleted; and records'
//! own encoding, which kimmydb's decoders check when a record is read.

use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::error::{RefusedKind, StorageError};

/// The most the copy-on-write view holds in memory. redb's writes during a
/// check are a few header pages, plus one repair commit's pages for a store
/// that was not closed cleanly; past this the check stops rather than grow.
pub const OVERLAY_CAP_BYTES: u64 = 64 << 20;

/// redb's default page cache, and the most the check sizes its cache to: a
/// store that fits is read once, not twice (the allocator rebuild re-reads
/// every page the verification read).
pub const CHECK_CACHE_CAP_BYTES: u64 = 1 << 30;

/// The view's unit of copy-on-write: redb's page size.
const BLOCK: u64 = 4096;

/// Exit statuses of `kimmyd check-store`. Not 2, which is clap's for a usage
/// error, nor 70 or 75, which a serving node's exits already mean.
pub const EXIT_CLEAN: i32 = 0;
pub const EXIT_DAMAGED: i32 = 65;
pub const EXIT_NOT_CHECKED: i32 = 1;

/// How the previous run of the data directory ended, as its exit marker
/// (`kimmy.last-exit`) says. The daemon reads the marker; this crate does not
/// know its format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PreviousRun {
    /// The store was closed cleanly: a clean shutdown, a restore, or a run
    /// that ended on an error after closing the store. Its next open repairs
    /// nothing.
    EndedCleanly,
    /// Anything else, a missing marker included: the store may not have been
    /// closed, and its last commit may be torn.
    #[default]
    NotClean,
}

/// The check's verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every page verified, and the open repaired nothing.
    Clean,
    /// The store was not closed cleanly; the open's repair kept every commit,
    /// and then every page verified.
    CleanAfterRepair,
    /// The store was not closed cleanly, and the open's repair rolled back
    /// its latest commit, which failed verification; what remains verified.
    /// The expected outcome of a torn write, and indistinguishable here from
    /// damage confined to that commit.
    RolledBackAfterUncleanStop,
    /// The store is damaged.
    Damaged,
    /// The check could not be made; nothing is known about the store.
    NotChecked,
}

impl Verdict {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::CleanAfterRepair => "clean_after_repair",
            Self::RolledBackAfterUncleanStop => "rolled_back_after_unclean_stop",
            Self::Damaged => "damaged",
            Self::NotChecked => "not_checked",
        }
    }

    pub const fn exit_code(self) -> i32 {
        match self {
            Self::Clean | Self::CleanAfterRepair | Self::RolledBackAfterUncleanStop => EXIT_CLEAN,
            Self::Damaged => EXIT_DAMAGED,
            Self::NotChecked => EXIT_NOT_CHECKED,
        }
    }
}

/// What a check found, and what it cost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckReport {
    pub verdict: Verdict,
    /// What was found, or why nothing could be: one sentence for an operator.
    pub detail: String,
    /// redb's open found the store not closed cleanly, and repaired it in
    /// memory.
    pub unclean_close: bool,
    /// That repair rolled back the latest commit.
    pub rolled_back: bool,
    pub elapsed_ms: u64,
    /// Bytes read from the file.
    pub bytes_read: u64,
    /// Writes that reached the file through the view. The view's descriptor
    /// is read-only and it has no path to the file for a write, so this is
    /// 0; a test holds it there, beside the file's digest and modification
    /// time.
    pub file_writes: u64,
}

impl CheckReport {
    /// The one line `kimmyd check-store` prints on stdout.
    pub fn json_line(&self) -> String {
        serde_json::json!({
            "verdict": self.verdict.label(),
            "detail": self.detail,
            "unclean_close": self.unclean_close,
            "rolled_back": self.rolled_back,
            "elapsed_ms": self.elapsed_ms,
            "bytes_read": self.bytes_read,
        })
        .to_string()
    }

    pub fn exit_code(&self) -> i32 {
        self.verdict.exit_code()
    }
}

/// How to check.
#[derive(Clone, Debug)]
pub struct CheckOptions {
    /// How the previous run of the data directory ended, read with the
    /// directory once the store's lock is held, so no start can write its
    /// exit marker between the read and the check. The daemon passes its
    /// reader of `kimmy.last-exit`; the default says [`PreviousRun::NotClean`].
    pub previous_run: fn(&Path) -> PreviousRun,
    /// Added to every read of the file: `KIMMY_TEST_CHECK_READ_MS`, so a test
    /// can act while a check is running. Zero otherwise.
    pub read_delay: Duration,
}

impl Default for CheckOptions {
    fn default() -> Self {
        CheckOptions { previous_run: |_| PreviousRun::NotClean, read_delay: Duration::ZERO }
    }
}

/// Where a re-seed or a restore is described, named in every damaged verdict.
const DAMAGED_WAY_OUT: &str = "Restore it from a backup with `kimmyd restore`, or on a cluster \
     member wipe the data directory and let it catch up from its peers (see operations.md, \"A \
     damaged store\")";

/// Check the store at `database`, which no process may have open, writing
/// nothing to it or beside it (see the module docs). A stop asked for with
/// [`crate::request_open_stop`] ends the check at its next read, as
/// [`Verdict::NotChecked`].
pub fn check_store(database: &Path, options: &CheckOptions) -> CheckReport {
    let started = Instant::now();
    let tally = Arc::new(Tally::default());
    let mut report = match check(database, options, &tally) {
        Ok(found) => found,
        Err(not_checked) => not_checked.into_report(),
    };
    // A read error, the cap or a stop decides the verdict however redb
    // reported it: none of them is damage. Only what happened before the
    // verdict counts; the tally is sealed before redb's close.
    if let Some(failure) = tally.failure() {
        report.verdict = Verdict::NotChecked;
        report.detail = failure;
    } else if report.verdict == Verdict::NotChecked
        && let Some(past_end) = tally.past_end()
    {
        // redb asked for bytes its own storage does not have: a page the
        // store's tree names lies past the end of the file, as when the file
        // was cut short. Decided by arithmetic before any read of the file,
        // so not an I/O error; a node's open reads the same bytes and fails.
        report.verdict = Verdict::Damaged;
        report.detail = format!(
            "redb's walk of it reached past the end of the file ({past_end}), so part of it is \
             missing. {DAMAGED_WAY_OUT}"
        );
    }
    match report.verdict {
        Verdict::Damaged => {
            warn!(path = %database.display(), detail = %report.detail, "the store is damaged")
        }
        Verdict::NotChecked => {
            warn!(path = %database.display(), detail = %report.detail, "the store was not checked")
        }
        _ => info!(
            path = %database.display(),
            verdict = report.verdict.label(),
            "the store verified"
        ),
    }
    report.elapsed_ms = started.elapsed().as_millis() as u64;
    report.bytes_read = tally.bytes_read.load(Ordering::Relaxed);
    report.file_writes = tally.file_writes.load(Ordering::Relaxed);
    report
}

/// A check that ended before a verdict, with why.
struct Found {
    verdict: Verdict,
    detail: String,
}

impl Found {
    fn damaged(detail: String) -> Self {
        Found { verdict: Verdict::Damaged, detail: format!("{detail}. {DAMAGED_WAY_OUT}") }
    }

    fn not_checked(detail: String) -> Self {
        Found { verdict: Verdict::NotChecked, detail }
    }

    fn into_report(self) -> CheckReport {
        CheckReport {
            verdict: self.verdict,
            detail: self.detail,
            unclean_close: false,
            rolled_back: false,
            elapsed_ms: 0,
            bytes_read: 0,
            file_writes: 0,
        }
    }
}

fn check(
    database: &Path,
    options: &CheckOptions,
    tally: &Arc<Tally>,
) -> Result<CheckReport, Found> {
    let shown = database.display();
    // No store is never created: the open below would lay one out.
    let len = match std::fs::metadata(database) {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Found::not_checked(format!("there is no store at {shown}")));
        }
        Err(e) => return Err(Found::not_checked(format!("{shown} could not be read: {e}"))),
    };
    if len == 0 {
        return Err(Found::not_checked(format!(
            "{shown} is empty: a start that died before it laid the store out leaves this, and \
             there is nothing in it to check"
        )));
    }

    // ADR-190's checks, removing nothing.
    let build = crate::format::BuildVersions::ours();
    let cleared = crate::format::check_reading_only(database, &build).map_err(refusal)?;
    if cleared.fresh() {
        return Err(Found::not_checked(format!("{shown} became empty while it was checked")));
    }

    // The store's lock, held until redb has closed the view. A read-only copy
    // cannot be locked: the range lock needs a descriptor open for writing.
    let (held, locked) = crate::store_lock::StoreLock::hold(database).map_err(|e| match e {
        redb::DatabaseError::DatabaseAlreadyOpen => Found::not_checked(format!(
            "{shown} is open in another process, so it was not checked; stop the node first"
        )),
        e => Found::not_checked(format!(
            "{shown} could not be opened and locked for the check ({e}); the check takes the \
             store's lock, which needs the file writable by this user, though it writes nothing"
        )),
    })?;
    cleared.confirm_under_lock(database, &locked).map_err(refusal)?;
    drop(locked);
    // Under the lock: a start that wrote its exit marker now would have had
    // to hold the store, which the lock refuses.
    let previous_run = (options.previous_run)(database.parent().unwrap_or(Path::new(".")));

    let view = CowBackend::open(database, Arc::clone(tally), options.read_delay)
        .map_err(|e| Found::not_checked(format!("{shown} could not be opened to read: {e}")))?;
    let unclean_close = Arc::new(AtomicBool::new(false));
    let rolled_back = Arc::new(AtomicBool::new(false));
    let mut builder = redb::Database::builder();
    builder.set_cache_size(len.clamp(1 << 20, CHECK_CACHE_CAP_BYTES) as usize);
    {
        let unclean_close = Arc::clone(&unclean_close);
        let rolled_back = Arc::clone(&rolled_back);
        builder.set_repair_callback(move |session| {
            let progress = session.progress();
            if progress == 0.0 {
                unclean_close.store(true, Ordering::Relaxed);
            } else if (progress - 0.3).abs() < 1e-9 {
                rolled_back.store(true, Ordering::Relaxed);
            }
        });
    }
    info!(path = %shown, bytes = len, "checking the store against redb's page checksums");
    // The third call that opens a store read-write, on a view that keeps every
    // write in memory (ADR-204). A panic in redb is a damaged store, as at
    // the engine's open.
    #[allow(clippy::disallowed_methods)]
    let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        builder.create_with_backend(view)
    }));
    let repairs = Repairs {
        unclean_close: unclean_close.load(Ordering::Relaxed),
        rolled_back: rolled_back.load(Ordering::Relaxed),
    };
    let found = match opened {
        Err(panic) => Found::damaged(redb_panicked(&*panic)),
        Ok(Err(e)) => from_redb("redb's open", e),
        Ok(Ok(mut db)) => {
            #[cfg(test)]
            test_hooks::before_integrity_check(database);
            let checked =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.check_integrity()));
            // The verdict is decided before the view is dropped: redb's close
            // writes the header once more, into memory, and a failure there
            // only warns.
            let found = match checked {
                Err(panic) => Found::damaged(redb_panicked(&*panic)),
                Ok(checked) => judge(checked, repairs, previous_run),
            };
            // Decided: a stop, the cap or a read past the end during redb's
            // close, which commits the allocator state into the view, changes
            // nothing now.
            tally.seal();
            #[cfg(test)]
            test_hooks::after_verdict();
            drop(db);
            found
        }
    };
    drop(held);
    Ok(CheckReport {
        verdict: found.verdict,
        detail: found.detail,
        unclean_close: repairs.unclean_close,
        rolled_back: repairs.rolled_back,
        elapsed_ms: 0,
        bytes_read: 0,
        file_writes: 0,
    })
}

/// What redb's open repaired, from its repair callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Repairs {
    unclean_close: bool,
    rolled_back: bool,
}

/// The verdict on an open that succeeded and the integrity check after it.
fn judge(
    checked: Result<bool, redb::DatabaseError>,
    repairs: Repairs,
    previous: PreviousRun,
) -> Found {
    match checked {
        // The open before it has already repaired anything an unclean stop
        // left, so what makes the check repair here is a saved allocator
        // state or a table count that does not match the tree, or a damaged
        // primary slot whose two-phase bit was cleared (the god byte that
        // holds it has no checksum), which the check rolls back to the
        // secondary. Damage, or a redb bug; never a clean store.
        Ok(false) => Found::damaged(
            "redb's integrity check would repair it: its saved allocator state or a table count \
             does not match the tree, or its latest commit fails verification on a store that \
             does not say it was committed in two phases"
                .into(),
        ),
        Ok(true) if !repairs.unclean_close => {
            Found { verdict: Verdict::Clean, detail: "every page verified".into() }
        }
        // A clean stop leaves nothing to repair: a repair now means the
        // header or the saved allocator state was changed after it.
        Ok(true) if previous == PreviousRun::EndedCleanly => Found::damaged(format!(
            "its previous run ended cleanly, yet redb's open had to repair it{}",
            if repairs.rolled_back { ", rolling back its latest commit" } else { "" }
        )),
        Ok(true) if repairs.rolled_back => Found {
            verdict: Verdict::RolledBackAfterUncleanStop,
            detail: "it was not closed cleanly, and its latest commit failed verification and was \
                     rolled back, as a torn write is; what remains verified. A start rolls it back \
                     the same way. The check cannot tell a torn write from damage in that commit"
                .into(),
        },
        Ok(true) => Found {
            verdict: Verdict::CleanAfterRepair,
            detail: "it was not closed cleanly; redb's repair kept every commit, and every page \
                     verified"
                .into(),
        },
        Err(e) => from_redb("redb's integrity check", e),
    }
}

/// The verdict on an error from redb: only `Corrupted` is damage.
fn from_redb(during: &str, error: redb::DatabaseError) -> Found {
    match error {
        redb::DatabaseError::Storage(redb::StorageError::Corrupted(why)) => {
            Found::damaged(format!("{during} found it damaged: {why}"))
        }
        e => Found::not_checked(format!("{during} failed: {e}")),
    }
}

/// The verdict on a refusal by the checks before the open.
fn refusal(error: StorageError) -> Found {
    match error {
        StorageError::RefusedStore { kind: RefusedKind::Damaged, why } => {
            // The refusal already names the way out.
            Found { verdict: Verdict::Damaged, detail: why }
        }
        StorageError::RefusedStore { kind: RefusedKind::Newer, why } => Found::not_checked(
            format!("a newer build wrote it, so this one cannot check it: {why}"),
        ),
        e => Found::not_checked(e.to_string()),
    }
}

fn redb_panicked(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .map(|m| m.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "no message".into());
    format!("redb panicked reading it ({message})")
}

/// What the view counted, the first failure that is not the store's, and the
/// first read past the end of the storage, which is.
#[derive(Debug, Default)]
struct Tally {
    bytes_read: AtomicU64,
    file_writes: AtomicU64,
    failure: Mutex<Option<String>>,
    past_end: Mutex<Option<String>>,
    /// Set once the verdict is decided: nothing after it is recorded.
    sealed: AtomicBool,
}

impl Tally {
    fn seal(&self) {
        self.sealed.store(true, Ordering::SeqCst);
    }

    fn sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    fn passed_end(&self, why: String) {
        if self.sealed() {
            return;
        }
        let mut past_end = self.past_end.lock().unwrap_or_else(|e| e.into_inner());
        if past_end.is_none() {
            *past_end = Some(why);
        }
    }

    fn past_end(&self) -> Option<String> {
        self.past_end.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn fail(&self, why: String) {
        if self.sealed() {
            return;
        }
        let mut failure = self.failure.lock().unwrap_or_else(|e| e.into_inner());
        if failure.is_none() {
            *failure = Some(why);
        }
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// A copy-on-write view of the store: reads come from the file, through a
/// read-only descriptor, except where redb has written, and every write
/// stays in memory, up to [`OVERLAY_CAP_BYTES`].
#[derive(Debug)]
struct CowBackend {
    file: std::fs::File,
    overlay: Mutex<Overlay>,
    answers: crate::store_lock::LockAnswers,
    tally: Arc<Tally>,
    read_delay: Duration,
}

#[derive(Debug)]
struct Overlay {
    /// The storage's length as redb sees it.
    len: u64,
    /// How much of the file still shows through: lowered by a shrink, so
    /// what lies past it reads as zeros if the storage grows again.
    base_limit: u64,
    /// Blocks redb has written, whole.
    blocks: HashMap<u64, Box<[u8]>>,
}

impl CowBackend {
    fn open(database: &Path, tally: Arc<Tally>, read_delay: Duration) -> std::io::Result<Self> {
        let file = std::fs::File::open(database)?;
        let len = file.metadata()?.len();
        Ok(CowBackend {
            file,
            overlay: Mutex::new(Overlay { len, base_limit: len, blocks: HashMap::new() }),
            answers: crate::store_lock::LockAnswers::default(),
            tally,
            read_delay,
        })
    }

    fn overlay(&self) -> std::sync::MutexGuard<'_, Overlay> {
        self.overlay.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Read `out.len()` bytes of the file at `offset`, all of which lie below
    /// `base_limit`. An error is the check's failure, never zeros.
    fn read_file(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        if crate::open_progress::open_stop_requested() {
            let why = "the check was stopped before it finished".to_string();
            self.tally.fail(why.clone());
            return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, why));
        }
        if !self.read_delay.is_zero() {
            std::thread::sleep(self.read_delay);
        }
        #[cfg(test)]
        let injected = test_hooks::injected_read_error();
        #[cfg(not(test))]
        let injected = None;
        let read = match injected {
            Some(error) => Err(error),
            None => self.file.read_exact_at(out, offset),
        };
        match read {
            Ok(()) => {
                self.tally.bytes_read.fetch_add(out.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.tally.fail(format!(
                    "reading {} bytes of the file at offset {offset} failed ({e}), so the check \
                     could not finish; this says nothing about the store's checksums",
                    out.len()
                ));
                Err(e)
            }
        }
    }

    /// Block `block` as the file shows it: zeros past `base_limit`.
    fn file_block(&self, block: u64, base_limit: u64) -> std::io::Result<Box<[u8]>> {
        let mut bytes = vec![0u8; BLOCK as usize].into_boxed_slice();
        let start = block * BLOCK;
        if start < base_limit {
            let n = (base_limit - start).min(BLOCK) as usize;
            self.read_file(start, &mut bytes[..n])?;
        }
        Ok(bytes)
    }
}

impl redb::StorageBackend for CowBackend {
    fn len(&self) -> std::result::Result<u64, std::io::Error> {
        Ok(self.overlay().len)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), std::io::Error> {
        let overlay = self.overlay();
        let end = offset.checked_add(out.len() as u64).filter(|&end| end <= overlay.len);
        let Some(end) = end else {
            let why = format!(
                "a read of {} bytes at offset {offset}, past the storage's {} bytes",
                out.len(),
                overlay.len
            );
            self.tally.passed_end(why.clone());
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, why));
        };
        // Most reads touch nothing redb wrote: one read of the file.
        let first = offset / BLOCK;
        let last = end.saturating_sub(1) / BLOCK;
        let untouched = !(first..=last).any(|b| overlay.blocks.contains_key(&b));
        if untouched && end <= overlay.base_limit {
            return self.read_file(offset, out);
        }
        let mut done = 0usize;
        while done < out.len() {
            let at = offset + done as u64;
            let block = at / BLOCK;
            let within = (at % BLOCK) as usize;
            let n = (BLOCK as usize - within).min(out.len() - done);
            match overlay.blocks.get(&block) {
                Some(bytes) => out[done..done + n].copy_from_slice(&bytes[within..within + n]),
                None => {
                    let shown = overlay.base_limit.saturating_sub(at).min(n as u64) as usize;
                    if shown > 0 {
                        self.read_file(at, &mut out[done..done + shown])?;
                    }
                    out[done + shown..done + n].fill(0);
                }
            }
            done += n;
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> std::result::Result<(), std::io::Error> {
        let mut overlay = self.overlay();
        if len < overlay.len {
            overlay.base_limit = overlay.base_limit.min(len);
            overlay.blocks.retain(|&block, _| block * BLOCK < len);
            let within = (len % BLOCK) as usize;
            if within != 0
                && let Some(bytes) = overlay.blocks.get_mut(&(len / BLOCK))
            {
                bytes[within..].fill(0);
            }
        }
        overlay.len = len;
        Ok(())
    }

    fn sync_data(&self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
        let mut overlay = self.overlay();
        let mut done = 0usize;
        while done < data.len() {
            let at = offset + done as u64;
            let block = at / BLOCK;
            let within = (at % BLOCK) as usize;
            let n = (BLOCK as usize - within).min(data.len() - done);
            if !overlay.blocks.contains_key(&block) {
                if (overlay.blocks.len() as u64 + 1) * BLOCK > OVERLAY_CAP_BYTES {
                    let why = format!(
                        "redb wrote more than the check holds in memory ({} MiB), so the check \
                         stopped",
                        OVERLAY_CAP_BYTES >> 20
                    );
                    self.tally.fail(why.clone());
                    return Err(std::io::Error::other(why));
                }
                let bytes = self.file_block(block, overlay.base_limit)?;
                overlay.blocks.insert(block, bytes);
            }
            let bytes = overlay.blocks.get_mut(&block).expect("inserted above");
            bytes[within..within + n].copy_from_slice(&data[done..done + n]);
            done += n;
        }
        // A write past the end extends the storage, as it would a file.
        overlay.len = overlay.len.max(offset + data.len() as u64);
        Ok(())
    }

    /// Nothing to release: the store's lock is held on another descriptor,
    /// released by the check once redb has closed, and never by this.
    fn close(&self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }

    fn try_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<bool, redb::BackendError> {
        self.answers.request(start, end, false)
    }

    fn try_lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<bool, redb::BackendError> {
        self.answers.request(start, end, true)
    }

    fn lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<(), redb::BackendError> {
        Err(crate::store_lock::unexpected("a blocking lock", start, end))
    }

    fn lock_shared_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<(), redb::BackendError> {
        Err(crate::store_lock::unexpected("a blocking shared lock", start, end))
    }

    fn unlock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<(), redb::BackendError> {
        self.answers.unlock(start, end)
    }

    fn query_lock_range(
        &self,
        start: std::ops::Bound<u64>,
        end: std::ops::Bound<u64>,
    ) -> std::result::Result<bool, redb::BackendError> {
        Err(crate::store_lock::unexpected("a lock query", start, end))
    }
}

/// What a test injects into a check, on the checking thread: a read error, and
/// a probe run while the view is open.
#[cfg(test)]
mod test_hooks {
    type Probe = Box<dyn FnMut(&std::path::Path)>;

    thread_local! {
        /// Reads of the file to let through before every later one fails.
        pub(super) static FAIL_READS_AFTER: std::cell::Cell<Option<u64>> =
            const { std::cell::Cell::new(None) };
        /// Run once redb has opened the view, before its integrity check.
        pub(super) static BEFORE_INTEGRITY_CHECK: std::cell::RefCell<Option<Probe>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(super) fn injected_read_error() -> Option<std::io::Error> {
        FAIL_READS_AFTER.with(|after| match after.get() {
            Some(0) => Some(std::io::Error::from_raw_os_error(libc::EIO)),
            Some(n) => {
                after.set(Some(n - 1));
                None
            }
            None => None,
        })
    }

    thread_local! {
        /// Ask for a stop once the verdict is decided, before redb's close.
        pub(super) static STOP_AFTER_VERDICT: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }

    pub(super) fn after_verdict() {
        if STOP_AFTER_VERDICT.with(|s| s.get()) {
            crate::open_progress::tests::THIS_THREAD_STOPS.with(|s| s.set(true));
        }
    }

    pub(super) fn before_integrity_check(path: &std::path::Path) {
        BEFORE_INTEGRITY_CHECK.with(|p| {
            if let Some(probe) = p.borrow_mut().as_mut() {
                probe(path);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use redb::StorageBackend;
    use std::path::PathBuf;

    /// Two values, each written in its own commit, the second the latest:
    /// ASCII, so a flipped low bit leaves a document that still decodes.
    const OLDER: &str = "older-marker-0123456789-abcdefghijklmnopqrstuvwxyz-end";
    const LATEST: &str = "latest-marker-9876543210-zyxwvutsrqponmlkjihgfedcba-end";

    fn fill(engine: &Engine) {
        let c = engine.create_collection("shop", "orders").unwrap();
        let docs = (0..500)
            .map(
                |i| bson::doc! { "_id": format!("d{i}"), "pad": format!("padding-{i:08}-padding") },
            )
            .collect();
        engine.insert_many(&c, docs).unwrap();
        engine.insert(&c, bson::doc! { "_id": "a", "v": OLDER }).unwrap();
        engine.insert(&c, bson::doc! { "_id": "b", "v": LATEST }).unwrap();
    }

    /// A store an engine closed cleanly.
    fn a_clean_store() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        fill(&Engine::open(&path).unwrap());
        assert!(crate::format::closed_cleanly(&path).unwrap());
        (dir, path)
    }

    /// A copy taken while an engine had the store open: one that was not
    /// closed, as a kill leaves it.
    fn an_unclean_store() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("live.redb");
        let path = dir.path().join("data").join("kimmy.redb");
        std::fs::create_dir(path.parent().unwrap()).unwrap();
        let engine = Engine::open(&live).unwrap();
        fill(&engine);
        std::fs::copy(&live, &path).unwrap();
        std::fs::copy(crate::format::sidecar_path(&live), crate::format::sidecar_path(&path))
            .unwrap();
        drop(engine);
        assert!(!crate::format::closed_cleanly(&path).unwrap());
        (dir, path)
    }

    /// Flip the low bit of a byte inside every copy of `needle` in the file,
    /// leaving the checksums as they were. Returns how many it found.
    fn flip(path: &Path, needle: &str) -> usize {
        let mut bytes = std::fs::read(path).unwrap();
        let at: Vec<usize> = bytes
            .windows(needle.len())
            .enumerate()
            .filter(|(_, w)| *w == needle.as_bytes())
            .map(|(i, _)| i)
            .collect();
        for i in &at {
            bytes[i + 16] ^= 1;
        }
        std::fs::write(path, &bytes).unwrap();
        at.len()
    }

    /// Every file in the directory: name, digest, length and modification
    /// time.
    fn listing(dir: &Path) -> Vec<(String, Vec<u8>, u64, std::time::SystemTime)> {
        use sha2::Digest;
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let meta = e.metadata().unwrap();
                let digest = if meta.is_file() {
                    sha2::Sha256::digest(std::fs::read(e.path()).unwrap()).to_vec()
                } else {
                    Vec::new()
                };
                (
                    e.file_name().to_string_lossy().into_owned(),
                    digest,
                    meta.len(),
                    meta.modified().unwrap(),
                )
            })
            .collect();
        files.sort();
        files
    }

    fn check_with(path: &Path, previous_run: PreviousRun) -> CheckReport {
        let previous_run: fn(&Path) -> PreviousRun = match previous_run {
            PreviousRun::EndedCleanly => |_| PreviousRun::EndedCleanly,
            PreviousRun::NotClean => |_| PreviousRun::NotClean,
        };
        check_store(path, &CheckOptions { previous_run, ..Default::default() })
    }

    /// Check, and hold the data directory to being exactly as it was: every
    /// file's digest, length and modification time, and no write reaching
    /// the file through the view.
    fn check_untouched(path: &Path, previous_run: PreviousRun) -> CheckReport {
        let dir = path.parent().unwrap();
        let before = listing(dir);
        let report = check_with(path, previous_run);
        assert_eq!(listing(dir), before, "the check changed the data directory: {report:?}");
        assert_eq!(report.file_writes, 0, "a write reached the file: {report:?}");
        report
    }

    #[test]
    fn a_clean_store_verifies_and_is_left_untouched() {
        let (_dir, path) = a_clean_store();
        // A temporary sidecar a dead writer left: a start removes it, and the
        // check, which writes nothing, must not.
        let stale = path.with_file_name(format!("kimmy.format.tmp.{}", i32::MAX - 1));
        std::fs::write(&stale, b"left by a writer that died").unwrap();
        assert!(!crate::format::process_is_running(i32::MAX - 1));
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        assert_eq!(report.verdict, Verdict::Clean, "{report:?}");
        assert_eq!(report.exit_code(), 0);
        assert!(!report.unclean_close && !report.rolled_back, "{report:?}");
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(report.bytes_read > 0 && report.bytes_read <= 2 * len, "{report:?}");
        // Clean whatever the marker says: nothing was repaired.
        assert_eq!(check_untouched(&path, PreviousRun::NotClean).verdict, Verdict::Clean);
    }

    /// The finding the check exists for: a flipped bit in a leaf of a store
    /// closed cleanly is served by an ordinary open, and the check finds it.
    #[test]
    fn a_flipped_bit_in_a_leaf_of_a_clean_store_opens_and_is_found_damaged_untouched() {
        let (dir, path) = a_clean_store();
        assert!(flip(&path, OLDER) > 0);
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        assert_eq!(report.verdict, Verdict::Damaged, "{report:?}");
        assert_eq!(report.exit_code(), 65);
        assert!(
            report.detail.contains("Primary is corrupted despite 2-phase commit"),
            "{report:?}"
        );
        assert!(report.detail.contains("kimmyd restore"), "{report:?}");

        let copy = dir.path().join("copy.redb");
        std::fs::copy(&path, &copy).unwrap();
        std::fs::copy(crate::format::sidecar_path(&path), crate::format::sidecar_path(&copy))
            .unwrap();
        let engine = Engine::open(&copy).expect("an ordinary open serves the damaged store");
        let c = engine.get_collection("shop", "orders").unwrap();
        let read = engine.get(&c, &kimmy_core::DocId::String("a".into())).unwrap().unwrap();
        assert_ne!(read.get_str("v").unwrap(), OLDER, "and reads the flipped value as data");
    }

    /// The two-phase bit is in the god byte, which no checksum covers. With
    /// it cleared on a store closed cleanly, redb's open repairs, treats a
    /// damaged primary as a torn commit and tries the secondary. A clean
    /// close's secondary shares the damaged page, so the store is still found
    /// damaged, whichever copy of the value was hit.
    #[test]
    fn a_damaged_leaf_with_the_two_phase_bit_cleared_is_still_damaged() {
        let (dir, path) = a_clean_store();
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes[9] & 4 != 0, "a clean close commits in two phases");
        let copies: Vec<usize> = bytes
            .windows(LATEST.len())
            .enumerate()
            .filter(|(_, w)| *w == LATEST.as_bytes())
            .map(|(i, _)| i)
            .collect();
        assert!(!copies.is_empty());
        for at in copies {
            let one = dir.path().join(format!("at-{at}"));
            std::fs::create_dir(&one).unwrap();
            let damaged = one.join("kimmy.redb");
            let mut edited = bytes.clone();
            edited[at + 16] ^= 1;
            edited[9] &= !4;
            std::fs::write(&damaged, &edited).unwrap();
            std::fs::copy(
                crate::format::sidecar_path(&path),
                crate::format::sidecar_path(&damaged),
            )
            .unwrap();
            let report = check_untouched(&damaged, PreviousRun::EndedCleanly);
            assert_eq!(report.verdict, Verdict::Damaged, "copy at {at}: {report:?}");
        }
    }

    #[test]
    fn an_unclean_store_is_repaired_in_memory_and_left_untouched() {
        let (_dir, path) = an_unclean_store();
        let report = check_untouched(&path, PreviousRun::NotClean);
        assert!(report.unclean_close && !report.rolled_back, "{report:?}");
        assert_eq!(report.verdict, Verdict::CleanAfterRepair, "{report:?}");
        assert_eq!(report.exit_code(), 0);
        assert!(!crate::format::closed_cleanly(&path).unwrap(), "still unrepaired on disk");
    }

    /// A store whose previous run ended cleanly needs no repair, so one that
    /// does is damaged, however the repair went.
    #[test]
    fn a_repair_after_a_clean_end_is_damage() {
        let (_dir, path) = an_unclean_store();
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        assert!(report.unclean_close, "{report:?}");
        assert_eq!(report.verdict, Verdict::Damaged, "{report:?}");
        assert_eq!(report.exit_code(), 65);
    }

    /// The latest commit of a store that was not closed, damaged: redb's
    /// open rolls it back, as it would a torn write. After an unclean end
    /// that is the expected case, exit 0; after a clean one it is damage.
    #[test]
    fn a_damaged_latest_commit_of_an_unclean_store_is_rolled_back_and_judged_by_the_last_exit() {
        let (_dir, path) = an_unclean_store();
        assert!(flip(&path, LATEST) > 0);
        let report = check_untouched(&path, PreviousRun::NotClean);
        assert!(report.unclean_close && report.rolled_back, "{report:?}");
        assert_eq!(report.verdict, Verdict::RolledBackAfterUncleanStop, "{report:?}");
        assert_eq!(report.exit_code(), 0);
        assert!(report.json_line().contains("\"verdict\":\"rolled_back_after_unclean_stop\""));

        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        assert!(report.rolled_back, "{report:?}");
        assert_eq!(report.verdict, Verdict::Damaged, "{report:?}");
        assert_eq!(report.exit_code(), 65);
    }

    #[test]
    fn a_store_held_open_is_not_checked_and_left_untouched() {
        let (_dir, path) = a_clean_store();
        let engine = Engine::open(&path).unwrap();
        let report = check_with(&path, PreviousRun::EndedCleanly);
        assert_eq!(report.verdict, Verdict::NotChecked, "{report:?}");
        assert_eq!(report.exit_code(), 1);
        assert!(report.detail.contains("open in another process"), "{report:?}");
        assert_eq!(report.bytes_read, 0, "{report:?}");
        drop(engine);
    }

    /// While a check runs, the store's lock is held: an engine's open in the
    /// middle of it is refused as in use, and the check is unaffected.
    #[test]
    fn an_open_during_a_check_is_refused_as_in_use() {
        let (_dir, path) = a_clean_store();
        let seen = std::rc::Rc::new(std::cell::RefCell::new(None));
        {
            let seen = std::rc::Rc::clone(&seen);
            test_hooks::BEFORE_INTEGRITY_CHECK.with(|p| {
                *p.borrow_mut() = Some(Box::new(move |path| {
                    *seen.borrow_mut() = Some(match Engine::open(path) {
                        Err(StorageError::StoreInUse(why)) => Ok(why),
                        Err(e) => Err(e.to_string()),
                        Ok(_) => Err("opened".to_string()),
                    });
                }));
            });
        }
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        test_hooks::BEFORE_INTEGRITY_CHECK.with(|p| p.borrow_mut().take());
        let refused = seen.borrow_mut().take().expect("the probe ran during the check");
        assert!(refused.unwrap().contains("open in another process"));
        assert_eq!(report.verdict, Verdict::Clean, "{report:?}");
    }

    #[test]
    fn a_missing_or_empty_store_is_not_checked_and_nothing_is_created() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let report = check_with(&path, PreviousRun::NotClean);
        assert_eq!(report.exit_code(), 1, "{report:?}");
        assert!(report.detail.contains("there is no store"), "{report:?}");
        assert!(listing(dir.path()).is_empty(), "nothing was created");

        std::fs::File::create(&path).unwrap();
        let before = listing(dir.path());
        let report = check_with(&path, PreviousRun::NotClean);
        assert_eq!(report.exit_code(), 1, "{report:?}");
        assert!(report.detail.contains("is empty"), "{report:?}");
        assert_eq!(listing(dir.path()), before, "nothing was laid out");
    }

    /// A store a newer build wrote is healthy: not checked, never damaged.
    #[test]
    fn a_store_a_newer_build_wrote_is_not_checked_rather_than_damaged() {
        let (_dir, path) = a_clean_store();
        let mut sidecar = crate::format::read_sidecar(&path).unwrap();
        sidecar.schema += 1;
        crate::format::write_sidecar(&path, &sidecar).unwrap();
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        assert_eq!(report.verdict, Verdict::NotChecked, "{report:?}");
        assert_eq!(report.exit_code(), 1);
        assert!(report.detail.contains("a newer build wrote it"), "{report:?}");
    }

    /// The checks before the open refuse a damaged header as damage.
    #[test]
    fn a_store_refused_as_damaged_before_the_open_is_damaged() {
        let (_dir, path) = a_clean_store();
        let mut bytes = std::fs::read(&path).unwrap();
        for slot in [64usize, 192] {
            bytes[slot + 1..slot + 128].fill(0xFF);
        }
        std::fs::write(&path, &bytes).unwrap();
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        assert_eq!(report.verdict, Verdict::Damaged, "{report:?}");
        assert_eq!(report.exit_code(), 65);
    }

    /// A read that fails is the check's failure, never zeros and never
    /// damage, wherever it lands: in the open or in the integrity check.
    #[test]
    fn a_read_error_is_not_checked_rather_than_damaged() {
        let (_dir, path) = a_clean_store();
        let reads = {
            let report = check_with(&path, PreviousRun::EndedCleanly);
            assert_eq!(report.verdict, Verdict::Clean);
            report.bytes_read
        };
        assert!(reads > 0);
        for after in [0u64, 1, 3, 10, 40] {
            test_hooks::FAIL_READS_AFTER.with(|f| f.set(Some(after)));
            let report = check_untouched(&path, PreviousRun::EndedCleanly);
            test_hooks::FAIL_READS_AFTER.with(|f| f.set(None));
            assert_eq!(report.verdict, Verdict::NotChecked, "after {after} reads: {report:?}");
            assert_eq!(report.exit_code(), 1);
            assert!(report.detail.starts_with("reading "), "the view's own account: {report:?}");
            assert!(report.detail.contains("says nothing about the store"), "{report:?}");
        }
    }

    /// redb's close commits the allocator state into the view, through the
    /// same reads that honour a stop and the cap; once the verdict is
    /// decided, neither changes it.
    #[test]
    fn a_stop_during_redbs_close_keeps_the_verdict() {
        let (_dir, path) = a_clean_store();
        test_hooks::STOP_AFTER_VERDICT.with(|s| s.set(true));
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        test_hooks::STOP_AFTER_VERDICT.with(|s| s.set(false));
        crate::open_progress::tests::THIS_THREAD_STOPS.with(|s| s.set(false));
        assert_eq!(report.verdict, Verdict::Clean, "{report:?}");
    }

    #[test]
    fn nothing_is_recorded_once_the_tally_is_sealed() {
        let tally = Tally::default();
        tally.passed_end("first".into());
        tally.passed_end("second".into());
        assert_eq!(tally.past_end().as_deref(), Some("first"), "the first read past the end");
        tally.seal();
        tally.fail("after".into());
        assert_eq!(tally.failure(), None);
        let sealed = Tally::default();
        sealed.seal();
        sealed.passed_end("after".into());
        assert_eq!(sealed.past_end(), None);
    }

    /// A store cut short: redb's repair walks to pages past the end of the
    /// file, which the view refuses before reading anything. Damage, exit 65.
    #[test]
    fn a_killed_store_cut_short_is_damaged() {
        let (_dir, path) = an_unclean_store();
        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 8 * BLOCK)
            .unwrap();
        let report = check_untouched(&path, PreviousRun::NotClean);
        assert_eq!(report.verdict, Verdict::Damaged, "{report:?}");
        assert_eq!(report.exit_code(), 65);
        assert!(report.detail.contains("past the end of the file"), "{report:?}");
        assert!(report.detail.contains("kimmyd restore"), "{report:?}");
    }

    /// The file shrinking under a running check is not the store's damage:
    /// the read of the file fails, exit 1, though redb's own length says the
    /// bytes are there.
    #[test]
    fn a_file_cut_short_during_the_check_is_not_checked() {
        let (_dir, path) = a_clean_store();
        test_hooks::BEFORE_INTEGRITY_CHECK.with(|p| {
            *p.borrow_mut() = Some(Box::new(|path| {
                let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
                file.set_len(file.metadata().unwrap().len() / 2).unwrap();
            }));
        });
        let report = check_with(&path, PreviousRun::EndedCleanly);
        test_hooks::BEFORE_INTEGRITY_CHECK.with(|p| p.borrow_mut().take());
        assert_eq!(report.verdict, Verdict::NotChecked, "{report:?}");
        assert!(report.detail.starts_with("reading "), "{report:?}");
    }

    thread_local! {
        static MARKER_READ_UNDER_LOCK: std::cell::Cell<Option<bool>> =
            const { std::cell::Cell::new(None) };
    }

    /// The exit marker is read once the store's lock is held: an engine's
    /// open at that moment is refused as in use.
    #[test]
    fn the_previous_run_is_read_under_the_stores_lock() {
        let (_dir, path) = a_clean_store();
        let options = CheckOptions {
            previous_run: |dir| {
                let held = matches!(
                    Engine::open(&dir.join("kimmy.redb")),
                    Err(StorageError::StoreInUse(_))
                );
                MARKER_READ_UNDER_LOCK.with(|m| m.set(Some(held)));
                PreviousRun::EndedCleanly
            },
            ..Default::default()
        };
        let report = check_store(&path, &options);
        assert_eq!(report.verdict, Verdict::Clean, "{report:?}");
        assert_eq!(MARKER_READ_UNDER_LOCK.with(|m| m.get()), Some(true), "read under the lock");
    }

    #[test]
    fn a_stop_ends_the_check_as_not_checked() {
        let (_dir, path) = a_clean_store();
        crate::open_progress::tests::THIS_THREAD_STOPS.with(|s| s.set(true));
        let report = check_untouched(&path, PreviousRun::EndedCleanly);
        crate::open_progress::tests::THIS_THREAD_STOPS.with(|s| s.set(false));
        assert_eq!(report.verdict, Verdict::NotChecked, "{report:?}");
        assert!(report.detail.starts_with("the check was stopped"), "{report:?}");
    }

    /// `Ok(false)` from redb's check is damage, not a clean store; the last
    /// exit decides only what the open repaired.
    #[test]
    fn the_verdicts_on_what_the_check_returned() {
        let none = Repairs { unclean_close: false, rolled_back: false };
        let repaired = Repairs { unclean_close: true, rolled_back: false };
        let rolled = Repairs { unclean_close: true, rolled_back: true };
        use PreviousRun::*;
        let cases = [
            (Ok(true), none, EndedCleanly, Verdict::Clean),
            (Ok(true), none, NotClean, Verdict::Clean),
            (Ok(true), repaired, NotClean, Verdict::CleanAfterRepair),
            (Ok(true), rolled, NotClean, Verdict::RolledBackAfterUncleanStop),
            (Ok(true), repaired, EndedCleanly, Verdict::Damaged),
            (Ok(true), rolled, EndedCleanly, Verdict::Damaged),
            (Ok(false), none, NotClean, Verdict::Damaged),
            (Ok(false), repaired, NotClean, Verdict::Damaged),
        ];
        for (checked, repairs, previous, want) in cases {
            assert_eq!(judge(checked, repairs, previous).verdict, want, "{repairs:?} {previous:?}");
        }
        let corrupted = redb::DatabaseError::Storage(redb::StorageError::Corrupted("x".into()));
        assert_eq!(judge(Err(corrupted), none, NotClean).verdict, Verdict::Damaged);
        let io = redb::DatabaseError::Storage(redb::StorageError::Io(std::io::Error::other("x")));
        assert_eq!(judge(Err(io), none, NotClean).verdict, Verdict::NotChecked);
    }

    fn a_view(len: usize) -> (tempfile::TempDir, CowBackend, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        let base: Vec<u8> = (0..len).map(|i| (i % 251) as u8 + 1).collect();
        std::fs::write(&path, &base).unwrap();
        let view = CowBackend::open(&path, Arc::default(), Duration::ZERO).unwrap();
        (dir, view, base)
    }

    fn read(view: &CowBackend, offset: u64, len: usize) -> Vec<u8> {
        let mut out = vec![0xEEu8; len];
        view.read(offset, &mut out).unwrap();
        out
    }

    #[test]
    fn the_view_reads_across_what_was_written_and_what_was_not() {
        let (dir, view, mut want) = a_view(5 * BLOCK as usize);
        let file = std::fs::read(dir.path().join("file")).unwrap();
        // A write straddling a block boundary, inside the file.
        view.write(BLOCK + 4000, &[7u8; 200]).unwrap();
        want[(BLOCK + 4000) as usize..(BLOCK + 4200) as usize].fill(7);
        assert_eq!(read(&view, 0, want.len()), want);
        assert_eq!(read(&view, BLOCK + 3990, 300), want[(BLOCK + 3990) as usize..][..300]);
        assert_eq!(read(&view, 3 * BLOCK, 10), want[3 * BLOCK as usize..][..10]);
        assert_eq!(std::fs::read(dir.path().join("file")).unwrap(), file, "the file is unchanged");
        let mut past = [0u8; 2];
        assert!(view.read(5 * BLOCK - 1, &mut past).is_err(), "a read past the end fails");
    }

    #[test]
    fn the_view_reads_zeros_where_it_shrank_and_grew_again() {
        let (_dir, view, base) = a_view(4 * BLOCK as usize);
        view.write(2 * BLOCK + 10, &[9u8; 100]).unwrap();
        // Shrink into the written block and into the file, then grow back.
        let cut = 2 * BLOCK + 50;
        view.set_len(cut).unwrap();
        assert_eq!(view.len().unwrap(), cut);
        view.set_len(4 * BLOCK).unwrap();
        let got = read(&view, 0, 4 * BLOCK as usize);
        assert_eq!(got[..(2 * BLOCK + 10) as usize], base[..(2 * BLOCK + 10) as usize]);
        assert!(got[(2 * BLOCK + 10) as usize..cut as usize].iter().all(|&b| b == 9));
        assert!(got[cut as usize..].iter().all(|&b| b == 0), "past the cut reads zeros");
        // A shrink below the written block, into the file alone.
        view.set_len(BLOCK / 2).unwrap();
        view.set_len(3 * BLOCK).unwrap();
        let got = read(&view, 0, 3 * BLOCK as usize);
        assert_eq!(got[..(BLOCK / 2) as usize], base[..(BLOCK / 2) as usize]);
        assert!(got[(BLOCK / 2) as usize..].iter().all(|&b| b == 0));
        // And a write past the end extends it.
        view.write(3 * BLOCK + 5, &[3u8; 10]).unwrap();
        assert_eq!(view.len().unwrap(), 3 * BLOCK + 15);
        assert_eq!(read(&view, 3 * BLOCK, 15), [&[0u8; 5][..], &[3u8; 10][..]].concat());
    }

    #[test]
    fn the_view_stops_at_its_memory_cap() {
        let (_dir, view, _) = a_view(BLOCK as usize);
        let blocks = OVERLAY_CAP_BYTES / BLOCK;
        view.set_len((blocks + 1) * BLOCK).unwrap();
        for b in 0..blocks {
            view.write(b * BLOCK, &[1]).unwrap();
        }
        assert!(view.write(blocks * BLOCK, &[1]).is_err(), "one block past the cap");
        assert!(view.tally.failure().unwrap().contains("holds in memory"));
    }
}
