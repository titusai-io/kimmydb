//! What the previous run of a data directory left behind, and what this one
//! leaves for the next.
//!
//! # Every exit is named in the log
//!
//! A node has three ways out of `node::run`: a signal, which logs `shutdown
//! signal received, draining` and then `shutdown complete`; an error, which
//! logs on the way out too; and **a supervised background task dying, which
//! writes a `task_died` marker and calls `process::exit(70)`**
//! ([ADR-184](../../../docs/decisions.md)) — that one is named here because
//! this module used to say there was no `process::exit` at all, which stopped
//! being true when supervision arrived. There is still no abort hook and no
//! `panic = "abort"`, and one further exception: a panic that unwinds out of
//! `main` exits 101 with
//! its message on stderr and writes no marker, so the next start reports it
//! as unclean, which is the truth. Otherwise a log that ends without one of
//! those lines and resumes at the startup banner is a process that was ended
//! from outside, by something that did not send it a signal it could log.
//! That happened to a member in 0.24.0, and the log said nothing at all
//! (ADR-147).
//!
//! This module is what makes the *next* such death say something. `run`
//! writes a small marker file into the data directory on its way out, on
//! both paths, and the next start reads and removes it. A start that finds a
//! database and no marker knows the previous run did not get to write one,
//! and says so at `WARN` — the one line an operator reading only the log
//! gets, and one they can alert on.
//!
//! The marker is written at the end rather than at the start (a pidfile
//! shape) so that its absence, not its presence, is the abnormal case: a
//! data directory copied, restored or first created has nothing in it and
//! reads as a first start, which is the right reading. `kimmyd restore`
//! writes one too, so a restored directory does not start with a warning
//! about a run that never happened.
//!
//! # A start that fails keeps what it inherited
//!
//! A start reads the marker by **renaming it aside**, to `kimmy.last-exit.previous`,
//! and removes that only once it is serving. A start that fails before then —
//! a store it refuses, a duty it cannot start, a port it cannot bind — writes
//! its own marker, and **carries the verdict it inherited** in it as
//! `previous`. Before, it read and deleted the marker at once, so a refused
//! start replaced the evidence of the run before it: a SIGKILL in the middle
//! of a migration was reported, two starts later, as a clean end (round 0380).
//!
//! **A `.previous` with no marker beside it is always an unclean end.** Only
//! a start that never reached serving and never wrote its own marker leaves
//! one — killed during the open, for instance — and the verdict it held is
//! reported as what came before that unclean end, not in place of it.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Filename of the exit marker inside the data directory, beside the
/// database file.
pub const LAST_EXIT_FILE: &str = "kimmy.last-exit";

/// Where a start keeps the marker it read until it is serving.
pub const PREVIOUS_FILE: &str = "kimmy.last-exit.previous";

/// How a run ended, as it says of itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exit {
    /// A signal was received and the drain completed.
    Shutdown,
    /// `node::run` returned an error, which it logged.
    Error,
    /// `kimmyd restore` wrote the database; there was no run.
    Restore,
    /// A supervised background task ended when it should not have, so the
    /// process stopped itself to be restarted (ADR-184). `task` and `cause` on
    /// the marker name which one and how.
    #[serde(rename = "task_died")]
    TaskDied,
    /// The storage engine hit an I/O error, after which it serves nothing, so
    /// the process stopped itself to be restarted (ADR-188). `cause` on the
    /// marker names the call and the error.
    #[serde(rename = "storage_failed")]
    StorageFailed,
}

impl Exit {
    pub fn name(self) -> &'static str {
        match self {
            Exit::Shutdown => "shutdown",
            Exit::Error => "error",
            Exit::Restore => "restore",
            Exit::TaskDied => "task_died",
            Exit::StorageFailed => "storage_failed",
        }
    }
}

/// The marker's contents: who wrote it, and when.
///
/// TOML, as the configuration is, so an operator with a shell can read it.
/// The identity here is the process's, not the node's: the node id lives in
/// the database and is the same for every run of this directory, whereas
/// the pid and the build are what distinguish the run that ended from the
/// one starting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastExit {
    pub exit: Exit,
    pub pid: u32,
    pub version: String,
    pub commit: String,
    /// Milliseconds since the Unix epoch, the stamp every other record in
    /// the data directory uses.
    pub at_ms: u64,
    /// Which supervised task died, for [`Exit::TaskDied`]. Optional on the
    /// wire and defaulted on read, so a marker written by a build without it
    /// still parses and an older build ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    /// How it died — `panicked` or `returned` — and, after a colon, the panic
    /// message or the error text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    /// Set on an [`Exit::Error`] written by a start that never reached
    /// serving: a refused start, as against a run that served and then
    /// ended on an error.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub failed_start: bool,
    /// What that failed start inherited: the verdict on the run before it,
    /// carried so a refusal does not erase it. A nested table, which a build
    /// before the field ignores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<Earlier>,
}

/// A verdict on an earlier run, carried through a start that failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Earlier {
    /// An [`Exit`] name, or `unclean` for a run that left no marker.
    pub exit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    /// For `unclean`: how long before the reading start the database was
    /// last written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_write_secs_ago: Option<u64>,
}

impl Earlier {
    fn of(last: &LastExit) -> Self {
        // A failed start that carried something passes on what it carried:
        // repeated refusals must not bury the last run that actually ran.
        if last.failed_start
            && let Some(earlier) = &last.previous
        {
            return earlier.clone();
        }
        Earlier {
            exit: last.exit.name().to_string(),
            version: Some(last.version.clone()),
            at_ms: Some(last.at_ms),
            cause: last.cause.clone(),
            last_write_secs_ago: None,
        }
    }
}

impl LastExit {
    fn now(exit: Exit) -> Self {
        let at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        Self {
            exit,
            pid: std::process::id(),
            version: kimmy_core::build::VERSION.to_string(),
            commit: kimmy_core::build::COMMIT.to_string(),
            at_ms,
            task: None,
            cause: None,
            failed_start: false,
            previous: None,
        }
    }
}

/// Record how this run ended, for the next start to read.
///
/// Best effort, and said so: a marker that cannot be written is a warning
/// here and a spurious `previous run did not shut down cleanly` on the next
/// start, both of which name the same directory. Silent when the directory
/// does not exist, which is a run that failed before it could create it and
/// has already logged why.
pub fn record_exit(data_dir: &Path, exit: Exit) {
    write_marker(data_dir, LastExit::now(exit), exit);
}

/// Record that this run ended on an error, with its text as the cause. A
/// start that failed before it was serving carries the verdict it inherited
/// (see the module docs), and leaves `.previous` to the next start only when
/// it could not write this.
pub fn record_error(data_dir: &Path, cause: &str) {
    let mut last = LastExit::now(Exit::Error);
    last.cause = Some(cause.to_string());
    if let Some(inherited) = inherited().remove(data_dir) {
        last.failed_start = true;
        last.previous = inherited;
    }
    if write_marker(data_dir, last, Exit::Error) {
        let _ = std::fs::remove_file(data_dir.join(PREVIOUS_FILE));
    }
}

/// The start has reached serving: what it inherited has been announced, and
/// is no longer carried by an exit of this run.
pub fn settle(data_dir: &Path) {
    inherited().remove(data_dir);
    match std::fs::remove_file(data_dir.join(PREVIOUS_FILE)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!(error = %e, "could not remove the previous run's exit marker"),
    }
}

/// The verdict each data directory's start inherited, until it is serving:
/// `None` for one that carries nothing (a first start), absent once settled or
/// never read. Keyed by directory, which a process has one of; the key keeps
/// tests that share a process apart.
fn inherited()
-> std::sync::MutexGuard<'static, std::collections::HashMap<std::path::PathBuf, Option<Earlier>>> {
    static INHERITED: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Option<Earlier>>>,
    > = std::sync::OnceLock::new();
    INHERITED.get_or_init(Default::default).lock().expect("never held across a panic")
}

/// Record that a supervised background task died, naming it and how (ADR-184).
///
/// Separate from [`record_exit`] because this is the one exit whose marker
/// carries more than the kind: the next start's `announce` names the task, and
/// that line is the only place an operator reliably reads why the process
/// restarted itself.
pub fn record_task_death(data_dir: &Path, task: &str, cause: &str) {
    let mut last = LastExit::now(Exit::TaskDied);
    last.task = Some(task.to_string());
    last.cause = Some(cause.to_string());
    write_marker(data_dir, last, Exit::TaskDied);
}

/// Record that the storage engine hit an I/O error (ADR-188), with the call
/// and the error as the cause.
///
/// A plain file write under the data directory and nothing else: it runs on
/// the thread that hit the error, and it must not touch the engine.
pub fn record_storage_failure(data_dir: &Path, cause: &str) {
    let mut last = LastExit::now(Exit::StorageFailed);
    last.cause = Some(cause.to_string());
    write_marker(data_dir, last, Exit::StorageFailed);
}

/// Write the marker atomically: a temporary file, synced, renamed over the
/// marker, and the directory synced. A marker cut short by a crash mid-write
/// used to be an unreadable one; now it is either the whole marker or none.
fn write_marker(data_dir: &Path, last: LastExit, exit: Exit) -> bool {
    if !data_dir.is_dir() {
        debug!(data_dir = %data_dir.display(), "no data directory to record the exit in");
        return false;
    }
    let path = data_dir.join(LAST_EXIT_FILE);
    let body = match toml::to_string(&last) {
        Ok(body) => body,
        Err(e) => {
            warn!(error = %e, "could not encode the exit marker");
            return false;
        }
    };
    match write_atomically(data_dir, &path, body.as_bytes()) {
        Ok(()) => true,
        Err(e) => {
            warn!(
                error = %e,
                path = %path.display(),
                exit = exit.name(),
                "could not write the exit marker; the next start will report an unclean exit"
            );
            false
        }
    }
}

fn write_atomically(dir: &Path, path: &Path, body: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let temp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(body)?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, path)?;
    // The rename is durable only once the directory is.
    std::fs::File::open(dir)?.sync_all()
}

/// What the data directory says about the run before this one.
#[derive(Debug, PartialEq, Eq)]
pub enum PreviousRun {
    /// No database and no marker: nothing ran here before.
    FirstStart,
    /// The marker was there, and said this.
    Ended(LastExit),
    /// A database with no marker beside it: the previous run did not reach
    /// the end of `node::run`. `last_write_secs_ago` is the database file's
    /// modification time, the cheapest available clue to when it stopped.
    /// `before` is the verdict a start that never reached serving held when
    /// it was ended: what came before this unclean end, not a substitute for
    /// it (H1).
    Unclean { last_write_secs_ago: Option<u64>, before: Option<Earlier> },
    /// A marker that could not be read or parsed. Not treated as clean — a
    /// run that wrote half a marker was still ended mid-write — and not as
    /// unclean either, because the file is evidence a run got that far.
    /// `removed` says whether it is gone, so the warning does not claim a
    /// removal a permission error refused and then repeat every start.
    Unreadable { error: String, removed: bool },
}

/// Read the previous run's marker, if any, and set it aside until this start
/// is serving (see the module docs).
///
/// The decision is returned rather than logged so it can be tested without a
/// subscriber; [`announce`] does the logging. What it says is also held for
/// [`record_error`], so a start that fails before [`settle`] carries it.
pub fn previous_run(data_dir: &Path, database: &Path) -> PreviousRun {
    let verdict = read_previous(data_dir, database);
    let carried = match &verdict {
        PreviousRun::FirstStart | PreviousRun::Unreadable { .. } => None,
        PreviousRun::Ended(last) => Some(Earlier::of(last)),
        PreviousRun::Unclean { last_write_secs_ago, .. } => Some(Earlier {
            exit: "unclean".to_string(),
            version: None,
            at_ms: None,
            cause: None,
            last_write_secs_ago: *last_write_secs_ago,
        }),
    };
    inherited().insert(data_dir.to_path_buf(), carried);
    verdict
}

fn read_previous(data_dir: &Path, database: &Path) -> PreviousRun {
    let path = data_dir.join(LAST_EXIT_FILE);
    let aside = data_dir.join(PREVIOUS_FILE);
    match std::fs::read_to_string(&path) {
        Ok(body) => {
            // Set aside before parsing, and never left in place: whatever it
            // says, it is about the run that ended, and a crash of this run
            // must not find it again as its own goodbye. The rename is atomic,
            // so there is no instant with neither file and a database.
            if let Err(e) = std::fs::rename(&path, &aside) {
                warn!(error = %e, path = %path.display(), "could not set the exit marker aside");
                let _ = std::fs::remove_file(&path);
            }
            match toml::from_str::<LastExit>(&body) {
                Ok(last) => PreviousRun::Ended(last),
                Err(e) => PreviousRun::Unreadable { error: e.to_string(), removed: true },
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A marker set aside by a start that neither reached serving nor
            // wrote its own: that start was ended from outside (H1). Always
            // unclean, whatever the set-aside marker says; what it says is what
            // came before.
            let before = std::fs::read_to_string(&aside)
                .ok()
                .and_then(|body| toml::from_str::<LastExit>(&body).ok())
                .map(|last| Earlier::of(&last));
            if !database.exists() && before.is_none() {
                return PreviousRun::FirstStart;
            }
            let last_write_secs_ago = std::fs::metadata(database)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs());
            PreviousRun::Unclean { last_write_secs_ago, before }
        }
        // Present but unreadable — a permission problem, most likely. Removed
        // all the same where that is possible, or the same warning would
        // repeat on every start.
        Err(e) => PreviousRun::Unreadable {
            error: e.to_string(),
            removed: std::fs::remove_file(&path).is_ok(),
        },
    }
}

/// Say what the previous run left, at the level it deserves.
///
/// One line per start. The unclean case is the whole reason this module
/// exists and is the only `WARN`; a clean marker is an `INFO` so that the
/// mechanism can be seen working, which is what makes the warning's absence
/// mean something.
pub fn announce(data_dir: &Path, previous: &PreviousRun) {
    match previous {
        PreviousRun::FirstStart => {
            debug!(data_dir = %data_dir.display(), "first start in this data directory");
        }
        // A task death is reported at warning and names the task, because it
        // is the one clean-marker case that is not a clean exit: the previous
        // run stopped itself to be restarted, and this line is where an
        // operator finds out why (ADR-184).
        PreviousRun::Ended(last) if last.exit == Exit::TaskDied => warn!(
            exit = last.exit.name(),
            task = last.task.as_deref().unwrap_or("unknown"),
            cause = last.cause.as_deref().unwrap_or("unknown"),
            previous_pid = last.pid,
            previous_version = %last.version,
            previous_commit = %last.commit,
            ended_at_ms = last.at_ms,
            "the previous run stopped itself because a background task ended"
        ),
        // The same kind of line, for the same reason: the previous run stopped
        // itself, and this is where an operator finds out why (ADR-188).
        PreviousRun::Ended(last) if last.exit == Exit::StorageFailed => warn!(
            exit = last.exit.name(),
            cause = last.cause.as_deref().unwrap_or("unknown"),
            previous_pid = last.pid,
            previous_version = %last.version,
            previous_commit = %last.commit,
            ended_at_ms = last.at_ms,
            "the previous run stopped itself because its storage engine hit an I/O error; \
             the database is repaired on this open"
        ),
        // An error is not a clean end, and a start that failed before it
        // served is not a run at all: said as such, with the error.
        PreviousRun::Ended(last) if last.exit == Exit::Error => {
            let what = if last.failed_start {
                "the previous start failed before it served"
            } else {
                "the previous run exited on an error"
            };
            warn!(
                exit = last.exit.name(),
                cause = last.cause.as_deref().unwrap_or("unknown"),
                previous_pid = last.pid,
                previous_version = %last.version,
                previous_commit = %last.commit,
                ended_at_ms = last.at_ms,
                "{what}"
            );
            if let Some(earlier) = &last.previous {
                announce_earlier(earlier);
            }
        }
        PreviousRun::Ended(last) => info!(
            exit = last.exit.name(),
            previous_pid = last.pid,
            previous_version = %last.version,
            previous_commit = %last.commit,
            ended_at_ms = last.at_ms,
            "previous run ended cleanly"
        ),
        PreviousRun::Unclean { last_write_secs_ago, before } => {
            warn!(
                data_dir = %data_dir.display(),
                last_database_write_secs_ago = last_write_secs_ago.map_or(-1, |s| s as i64),
                "previous run did not shut down cleanly: the database is here and the exit \
                 marker is not, so the process was ended by something that did not let it log \
                 its exit. Look at the container runtime and the kernel log for that time, and \
                 at kimmy_process_resident_peak_bytes against the memory limit"
            );
            if let Some(earlier) = before {
                announce_earlier(earlier);
            }
        }
        PreviousRun::Unreadable { error, removed } => warn!(
            error = %error,
            data_dir = %data_dir.display(),
            removed,
            "the exit marker in the data directory could not be read"
        ),
    }
}

/// The second line, for a verdict carried from before the one just reported.
fn announce_earlier(earlier: &Earlier) {
    if earlier.exit == "unclean" {
        warn!(
            last_database_write_secs_ago = earlier.last_write_secs_ago.map_or(-1, |s| s as i64),
            "and the run before it did not shut down cleanly"
        );
    } else {
        warn!(
            exit = %earlier.exit,
            cause = earlier.cause.as_deref().unwrap_or(""),
            earlier_version = earlier.version.as_deref().unwrap_or("unknown"),
            ended_at_ms = earlier.at_ms.unwrap_or(0),
            "and before that, the run ended with exit {}",
            earlier.exit
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_with_database() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kimmy.redb");
        std::fs::write(&db, b"not a real database").unwrap();
        (dir, db)
    }

    #[test]
    fn a_recorded_exit_is_read_back_once_and_then_gone() {
        let (dir, db) = dir_with_database();
        record_exit(dir.path(), Exit::Shutdown);
        assert!(dir.path().join(LAST_EXIT_FILE).exists());

        let previous = previous_run(dir.path(), &db);
        let PreviousRun::Ended(last) = previous else { panic!("{previous:?}") };
        assert_eq!(last.exit, Exit::Shutdown);
        assert_eq!(last.pid, std::process::id());
        assert_eq!(last.version, kimmy_core::build::VERSION);
        assert!(last.at_ms > 0);

        // Consumed: set aside, so a crash of this run cannot read as clean on
        // the next start.
        assert!(!dir.path().join(LAST_EXIT_FILE).exists());
        assert!(dir.path().join(PREVIOUS_FILE).exists(), "set aside until serving");
        settle(dir.path());
        assert!(!dir.path().join(PREVIOUS_FILE).exists(), "and gone once serving");
        assert_eq!(
            previous_run(dir.path(), &db),
            PreviousRun::Unclean { last_write_secs_ago: Some(0), before: None }
        );
    }

    #[test]
    fn a_database_with_no_marker_is_an_unclean_exit_and_an_empty_directory_is_a_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("kimmy.redb");
        assert_eq!(previous_run(dir.path(), &db), PreviousRun::FirstStart);

        std::fs::write(&db, b"x").unwrap();
        match previous_run(dir.path(), &db) {
            PreviousRun::Unclean { last_write_secs_ago: Some(secs), before: None } => {
                assert!(secs < 60)
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn every_exit_kind_round_trips_under_its_name() {
        // The name is what an operator greps the marker or the log for, so
        // it is part of the contract rather than an enum's Debug output.
        for (exit, name) in
            [(Exit::Shutdown, "shutdown"), (Exit::Error, "error"), (Exit::Restore, "restore")]
        {
            let (dir, db) = dir_with_database();
            record_exit(dir.path(), exit);
            let body = std::fs::read_to_string(dir.path().join(LAST_EXIT_FILE)).unwrap();
            assert!(body.contains(&format!("exit = \"{name}\"")), "{body}");
            assert_eq!(exit.name(), name);
            let PreviousRun::Ended(last) = previous_run(dir.path(), &db) else { panic!() };
            assert_eq!(last.exit, exit);
        }
    }

    #[test]
    fn a_garbage_marker_is_neither_clean_nor_unclean_and_is_removed() {
        let (dir, db) = dir_with_database();
        std::fs::write(dir.path().join(LAST_EXIT_FILE), "exit = 7\n").unwrap();
        assert!(matches!(previous_run(dir.path(), &db), PreviousRun::Unreadable { .. }));
        assert!(!dir.path().join(LAST_EXIT_FILE).exists());
    }

    #[test]
    fn recording_into_a_directory_that_does_not_exist_is_a_no_op() {
        // A run that failed before creating its data directory has logged
        // why; a second error about a marker would say nothing new.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("never-created");
        record_exit(&missing, Exit::Error);
        assert!(!missing.exists());
    }

    /// H1: a start that set the marker aside and was then ended before it
    /// served or wrote its own — killed during the open — leaves `.previous`
    /// alone. The next start reports an unclean end, with the set-aside
    /// verdict as what came before it, never that verdict in its place.
    #[test]
    fn a_start_killed_before_serving_is_unclean_with_the_earlier_verdict_after_it() {
        let (dir, db) = dir_with_database();
        record_exit(dir.path(), Exit::Shutdown);
        let _ = previous_run(dir.path(), &db); // the start that is then killed
        match previous_run(dir.path(), &db) {
            PreviousRun::Unclean { before: Some(before), .. } => {
                assert_eq!(before.exit, "shutdown", "{before:?}")
            }
            other => panic!("a killed start must read unclean: {other:?}"),
        }
    }

    /// A start that fails before serving carries what it inherited into its
    /// own marker, and the next start reads both.
    #[test]
    fn a_failed_start_carries_the_verdict_it_inherited() {
        let (dir, db) = dir_with_database();
        record_exit(dir.path(), Exit::Shutdown);
        let _ = previous_run(dir.path(), &db);
        record_error(dir.path(), "format 5 is not supported");
        assert!(!dir.path().join(PREVIOUS_FILE).exists(), "carried, so not left aside");
        let body = std::fs::read_to_string(dir.path().join(LAST_EXIT_FILE)).unwrap();
        assert!(body.contains("failed_start = true"), "{body}");
        assert!(body.contains("[previous]"), "a nested table: {body}");

        let PreviousRun::Ended(last) = previous_run(dir.path(), &db) else { panic!() };
        assert_eq!(last.exit, Exit::Error);
        assert_eq!(last.cause.as_deref(), Some("format 5 is not supported"));
        assert_eq!(last.previous.as_ref().map(|p| p.exit.as_str()), Some("shutdown"));
    }

    /// Repeated refusals keep the last run that actually ran, not the refusal
    /// before them.
    #[test]
    fn repeated_failed_starts_keep_the_last_real_run() {
        let (dir, db) = dir_with_database();
        let _ = previous_run(dir.path(), &db); // no marker, a database: unclean
        record_error(dir.path(), "refused once");
        let _ = previous_run(dir.path(), &db);
        record_error(dir.path(), "refused twice");
        let PreviousRun::Ended(last) = previous_run(dir.path(), &db) else { panic!() };
        assert_eq!(last.cause.as_deref(), Some("refused twice"));
        let earlier = last.previous.expect("carried");
        assert_eq!(earlier.exit, "unclean", "the unclean end survives two refusals: {earlier:?}");
    }

    /// A start that reached serving carries nothing into a later error: its
    /// inheritance was announced, and the error is the run's own.
    #[test]
    fn an_error_after_serving_carries_nothing() {
        let (dir, db) = dir_with_database();
        record_exit(dir.path(), Exit::Shutdown);
        let _ = previous_run(dir.path(), &db);
        settle(dir.path());
        record_error(dir.path(), "the listener failed");
        let PreviousRun::Ended(last) = previous_run(dir.path(), &db) else { panic!() };
        assert!(!last.failed_start && last.previous.is_none(), "{last:?}");
    }

    /// The marker is written whole or not at all: nothing temporary is left.
    #[test]
    fn a_marker_write_leaves_no_temporary_file() {
        let (dir, _db) = dir_with_database();
        record_exit(dir.path(), Exit::Shutdown);
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|n| !n.ends_with(".tmp")), "{names:?}");
    }
}
