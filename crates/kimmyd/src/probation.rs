//! Reading what probation is decided from, before the store opens (ADR-213).
//!
//! The decision itself is `kimmy_cluster::yielding::probation::decide`, a pure
//! function. This reads its inputs, all before serving and **the sidecar before the
//! open rewrites it**: the previous run's verdict (what `lifecycle` read), the free
//! space on the data directory's filesystem (`statvfs`, available bytes to the
//! unprivileged user, so ENOSPC and zero free coincide), the previous start's
//! record, and the store's format sidecar.

use std::path::Path;

use kimmy_cluster::yielding::probation::{Decision, Inputs, LastStart, PreviousEnd, decide};

use crate::lifecycle::{self, Exit, PreviousRun};

/// The previous run's verdict, as probation reads it.
///
/// **A start that failed before it served carries the verdict it inherited**
/// (`previous` on its marker), and that is the verdict used, not the `error` the
/// failed start wrote: repeated refusals must not bury the last run that actually
/// ran, which is the evidence probation wants.
pub fn previous_end(previous: &PreviousRun) -> PreviousEnd {
    match previous {
        PreviousRun::FirstStart => PreviousEnd::FirstStart,
        PreviousRun::Ended(last) => {
            if last.failed_start
                && let Some(earlier) = &last.previous
            {
                return from_name(&earlier.exit, None);
            }
            match last.exit {
                Exit::Shutdown => PreviousEnd::Shutdown,
                Exit::Error => PreviousEnd::Error,
                Exit::Restore => PreviousEnd::Restore,
                Exit::TaskDied => PreviousEnd::TaskDied,
                Exit::StorageFailed => PreviousEnd::StorageFailed,
                Exit::StorageNotClosed => PreviousEnd::StorageNotClosed,
            }
        }
        PreviousRun::Unclean { before, .. } => {
            PreviousEnd::Unclean { before: before.as_ref().map(|earlier| earlier.exit.clone()) }
        }
        PreviousRun::Unreadable { .. } => PreviousEnd::Unreadable,
    }
}

fn from_name(name: &str, before: Option<String>) -> PreviousEnd {
    match name {
        "shutdown" => PreviousEnd::Shutdown,
        "error" => PreviousEnd::Error,
        "restore" => PreviousEnd::Restore,
        "task_died" => PreviousEnd::TaskDied,
        "storage_failed" => PreviousEnd::StorageFailed,
        "storage_not_closed" => PreviousEnd::StorageNotClosed,
        "unclean" => PreviousEnd::Unclean { before },
        "unreadable" => PreviousEnd::Unreadable,
        "first_start" => PreviousEnd::FirstStart,
        _ => PreviousEnd::Error,
    }
}

/// The two sizes from a `statvfs` answer: the bytes **available to the unprivileged
/// user** (`f_bavail`, not the root-visible `f_bfree`, which counts the reserved
/// blocks the node cannot write into), and the filesystem's, both in `f_frsize`
/// units.
#[cfg(unix)]
fn sizes_of(stat: &libc::statvfs) -> (Option<u64>, Option<u64>) {
    // The field widths differ by platform (32-bit blocks on some, 64 on others),
    // so the conversions are needed on some targets and the identity on others.
    #[allow(clippy::useless_conversion)]
    let unit = u64::from(stat.f_frsize);
    #[allow(clippy::useless_conversion)]
    let (available, blocks) = (u64::from(stat.f_bavail), u64::from(stat.f_blocks));
    (Some(available.saturating_mul(unit)), Some(blocks.saturating_mul(unit)))
}

/// Bytes available to the unprivileged user, and the filesystem's size, for the
/// filesystem holding `dir`. `None` where it cannot be read (or off unix).
pub fn free_space(dir: &Path) -> (Option<u64>, Option<u64>) {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
            return (None, None);
        };
        // SAFETY: `statvfs` writes only into the struct handed to it, which is
        // zeroed and lives for the call; `path` is a valid NUL-terminated string.
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
            return (None, None);
        }
        sizes_of(&stat)
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        (None, None)
    }
}

/// The `written_by` the store's format sidecar names **as it is now**, before this
/// start opens the store and rewrites it to this build's.
pub fn sidecar_written_by(database: &Path) -> Option<String> {
    let body = std::fs::read_to_string(kimmy_storage::format::sidecar_path(database)).ok()?;
    toml::from_str::<kimmy_storage::format::Sidecar>(&body).ok().map(|s| s.written_by)
}

/// Gather the inputs and decide.
pub fn decide_for_start(
    data_dir: &Path,
    database: &Path,
    previous: &PreviousRun,
    now_ms: u64,
) -> (Decision, Inputs) {
    let (free_available, fs_size) = free_space(data_dir);
    let last_write_secs_ago = match previous {
        PreviousRun::Unclean { last_write_secs_ago, .. } => *last_write_secs_ago,
        _ => None,
    };
    let inputs = Inputs {
        end: previous_end(previous),
        free_available,
        fs_size,
        last_start: lifecycle::read_last_start(data_dir).map(|record| LastStart {
            at_ms: record.at_ms,
            inherited: record.inherited,
            written_by: record.written_by,
        }),
        sidecar_written_by: sidecar_written_by(database),
        now_ms,
        last_write_secs_ago,
    };
    let decision = decide(&inputs);
    (decision, inputs)
}

/// The one `INFO` line every start logs, naming each input and what it decided:
/// `probation: yes` or `probation: no`.
pub fn announce(decision: &Decision, inputs: &Inputs) {
    let run = decision
        .run_length
        .map_or_else(|| "unknown".to_string(), |length| format!("{}", length.as_secs()));
    let last_start = match (&inputs.last_start, decision.last_start_used) {
        (None, _) => "absent".to_string(),
        (Some(_), true) => "used".to_string(),
        (Some(record), false) => format!(
            "ignored: written by {}, the store's sidecar says {}",
            record.written_by,
            inputs.sidecar_written_by.as_deref().unwrap_or("nothing")
        ),
    };
    let inherited = inputs
        .last_start
        .as_ref()
        .filter(|_| decision.last_start_used)
        .map_or("unknown", |record| record.inherited.as_str());
    tracing::info!(
        verdict = inputs.end.label(),
        free_bytes = ?inputs.free_available,
        filesystem_bytes = ?inputs.fs_size,
        previous_run_secs = %run,
        inherited,
        last_start = %last_start,
        reasons = ?decision.reasons,
        "probation: {}",
        if decision.probation { "yes" } else { "no" }
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{Earlier, LastExit};

    fn marker(exit: Exit) -> LastExit {
        LastExit {
            exit,
            pid: 1,
            version: "0.44.0".into(),
            commit: "x".into(),
            at_ms: 1,
            task: None,
            cause: None,
            failed_start: false,
            previous: None,
        }
    }

    #[test]
    fn every_verdict_maps_to_its_own() {
        for (exit, want) in [
            (Exit::Shutdown, PreviousEnd::Shutdown),
            (Exit::Error, PreviousEnd::Error),
            (Exit::Restore, PreviousEnd::Restore),
            (Exit::TaskDied, PreviousEnd::TaskDied),
            (Exit::StorageFailed, PreviousEnd::StorageFailed),
            (Exit::StorageNotClosed, PreviousEnd::StorageNotClosed),
        ] {
            assert_eq!(previous_end(&PreviousRun::Ended(marker(exit))), want);
        }
        assert_eq!(previous_end(&PreviousRun::FirstStart), PreviousEnd::FirstStart);
        assert_eq!(
            previous_end(&PreviousRun::Unreadable { error: "x".into(), removed: true }),
            PreviousEnd::Unreadable
        );
        assert_eq!(
            previous_end(&PreviousRun::Unclean {
                last_write_secs_ago: Some(3),
                before: Some(Earlier {
                    exit: "task_died".into(),
                    version: None,
                    at_ms: None,
                    cause: None,
                    last_write_secs_ago: None,
                }),
            }),
            PreviousEnd::Unclean { before: Some("task_died".into()) }
        );
    }

    /// The cluster crate's mirror of `PreviousRun` cannot
    /// drift from the lifecycle's own names. For every [`Exit`] the mirror's label is
    /// the exit's name, whether the marker is read directly or carried by a failed
    /// start; the names a carried verdict can also hold (`unclean`, `unreadable`,
    /// `first_start`) read back to their own labels; an unknown name is an `error`.
    #[test]
    fn the_mirror_of_the_lifecycle_verdicts_cannot_drift() {
        // Exhaustive over `Exit`: a new variant stops this from compiling.
        fn every_exit() -> Vec<Exit> {
            let all = [
                Exit::Shutdown,
                Exit::Error,
                Exit::Restore,
                Exit::TaskDied,
                Exit::StorageFailed,
                Exit::StorageNotClosed,
            ];
            for exit in all {
                match exit {
                    Exit::Shutdown
                    | Exit::Error
                    | Exit::Restore
                    | Exit::TaskDied
                    | Exit::StorageFailed
                    | Exit::StorageNotClosed => {}
                }
            }
            all.to_vec()
        }
        let earlier = |name: &str| Earlier {
            exit: name.into(),
            version: None,
            at_ms: None,
            cause: None,
            last_write_secs_ago: None,
        };
        for exit in every_exit() {
            let direct = previous_end(&PreviousRun::Ended(marker(exit)));
            assert_eq!(direct.label(), exit.name(), "{exit:?} read directly");
            let mut failed = marker(Exit::Error);
            failed.failed_start = true;
            failed.previous = Some(earlier(exit.name()));
            let carried = previous_end(&PreviousRun::Ended(failed));
            assert_eq!(carried.label(), exit.name(), "{exit:?} carried by a failed start");
        }
        for name in ["unclean", "unreadable", "first_start"] {
            let mut failed = marker(Exit::Error);
            failed.failed_start = true;
            failed.previous = Some(earlier(name));
            let carried = previous_end(&PreviousRun::Ended(failed));
            assert_eq!(carried.label(), name, "carried");
        }
        let mut failed = marker(Exit::Shutdown);
        failed.failed_start = true;
        failed.previous = Some(earlier("a name from a later build"));
        assert_eq!(previous_end(&PreviousRun::Ended(failed)).label(), "error");
    }

    /// A refused start carries the verdict it inherited, and that is what probation
    /// reads: a node that fails to start over a full disk must not read as having
    /// ended on a plain error.
    #[test]
    fn a_failed_start_passes_on_the_verdict_it_carried() {
        let mut failed = marker(Exit::Error);
        failed.failed_start = true;
        failed.previous = Some(Earlier {
            exit: "storage_failed".into(),
            version: None,
            at_ms: None,
            cause: None,
            last_write_secs_ago: None,
        });
        assert_eq!(previous_end(&PreviousRun::Ended(failed)), PreviousEnd::StorageFailed);
        let mut bare = marker(Exit::Error);
        bare.failed_start = true;
        assert_eq!(previous_end(&PreviousRun::Ended(bare)), PreviousEnd::Error);
    }

    /// The available figure is `f_bavail`, and `f_bfree` (which
    /// counts blocks reserved for root) is not what the node can write into.
    #[cfg(unix)]
    #[test]
    fn the_available_bytes_are_the_unprivileged_ones() {
        // SAFETY: a plain-data C struct, valid all zero.
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        stat.f_frsize = 4096;
        stat.f_blocks = 1000;
        stat.f_bfree = 400;
        stat.f_bavail = 100;
        assert_eq!(sizes_of(&stat), (Some(100 * 4096), Some(1000 * 4096)));
    }

    #[test]
    fn free_space_reads_a_real_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let (available, size) = free_space(dir.path());
        let (available, size) = (available.expect("available"), size.expect("size"));
        assert!(size > 0 && available <= size);
        assert_eq!(free_space(&dir.path().join("nothing-here")), (None, None));
    }

    /// The sidecar is read as it stands, and a missing or unreadable one is none.
    #[test]
    fn the_sidecar_is_read_before_the_open_rewrites_it() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("kimmy.redb");
        assert_eq!(sidecar_written_by(&database), None);
        std::fs::write(
            kimmy_storage::format::sidecar_path(&database),
            "schema = 4\nredb_version = \"4.1\"\nredb_file_format = 3\nwritten_by = \"0.43.0\"\n",
        )
        .unwrap();
        assert_eq!(sidecar_written_by(&database).as_deref(), Some("0.43.0"));
        std::fs::write(kimmy_storage::format::sidecar_path(&database), "not toml {").unwrap();
        assert_eq!(sidecar_written_by(&database), None);
    }

    /// The start record round trips and a torn one reads as none. (The cleaning of a
    /// dead process's temporary is in the lifecycle tests.)
    #[test]
    fn the_start_record_round_trips_and_a_torn_one_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(lifecycle::read_last_start(dir.path()), None);
        lifecycle::record_start(dir.path(), "abcd", "unclean");
        let record = lifecycle::read_last_start(dir.path()).expect("written");
        assert_eq!((record.boot.as_str(), record.inherited.as_str()), ("abcd", "unclean"));
        assert_eq!(record.written_by, kimmy_core::build::VERSION);
        assert!(record.at_ms > 1_600_000_000_000);
        std::fs::write(dir.path().join(lifecycle::LAST_START_FILE), "at_ms = ").unwrap();
        assert_eq!(lifecycle::read_last_start(dir.path()), None, "torn: both conditions false");
    }

    /// The pure decision through the real readers: an ordinary 137 after a long run
    /// on a disk with space is not probation; the same with a recent start record is.
    #[test]
    fn the_decision_reads_the_record_and_the_sidecar_it_finds() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("kimmy.redb");
        std::fs::write(
            kimmy_storage::format::sidecar_path(&database),
            format!(
                "schema = 4\nredb_version = \"4.1\"\nredb_file_format = 3\nwritten_by = \"{}\"\n",
                kimmy_core::build::VERSION
            ),
        )
        .unwrap();
        let unclean = PreviousRun::Unclean { last_write_secs_ago: Some(5), before: None };
        lifecycle::record_start(dir.path(), "b1", "shutdown");
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as u64;
        // Just started: a short run.
        let (decision, inputs) = decide_for_start(dir.path(), &database, &unclean, now + 1_000);
        assert!(decision.probation, "{decision:?} {inputs:?}");
        assert!(decision.reasons.contains(&"short previous run"));
        // An hour on: an ordinary roll.
        let (decision, _) = decide_for_start(dir.path(), &database, &unclean, now + 3_600_000);
        assert!(!decision.probation, "{decision:?}");
    }
}
