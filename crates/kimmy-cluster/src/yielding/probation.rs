//! Whether a start begins in probation (ADR-213, ADR-188).
//!
//! **Pure**: the daemon reads the inputs before it serves (the previous run's
//! verdict, the free space, `kimmy.last-start` and the store's format sidecar) and
//! asks here. Probation makes every class start `stalled`, yielding from the first
//! block; ordinary rolls never trigger it.
//!
//! A start begins in probation when:
//! 1. the previous run ended `storage_failed` or `task_died`: **unconditionally**; or
//! 2. it ended `unclean` or `storage_not_closed`, **and at least one** of:
//!    - (a) free space is below [`low_space`];
//!    - (b) the previous run was shorter than [`super::SHORT_RUN`], measured from
//!      `kimmy.last-start`'s `at_ms` to now (never from the last write, which on an
//!      idle member is the last commit and not the death). A missing, unusable or
//!      clock-skewed record makes it false, never "short";
//!    - (c) a second non-clean end in a row: the record's `inherited`, or the `before`
//!      an unclean verdict carries, is itself non-clean.
//!
//! `error`, `unreadable`, `shutdown`, `restore` and `first_start` never do.
//!
//! **A `kimmy.last-start` is used only when its `written_by` equals the format
//! sidecar's** as read before this start rewrites it: a 0.43 build leaves the file
//! alone, so after a rollback and a roll forward it describes a run two binaries
//! ago.

use std::time::Duration;

use super::{LOW_SPACE_CAP, SHORT_RUN};

/// How the run before this one ended, as the daemon read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreviousEnd {
    FirstStart,
    Shutdown,
    Error,
    Restore,
    Unreadable,
    TaskDied,
    StorageFailed,
    StorageNotClosed,
    /// A database with no marker: the previous run never reached the end of `run`.
    /// `before` is the exit name a start that never reached serving inherited.
    Unclean {
        before: Option<String>,
    },
}

impl PreviousEnd {
    pub fn label(&self) -> &'static str {
        match self {
            Self::FirstStart => "first_start",
            Self::Shutdown => "shutdown",
            Self::Error => "error",
            Self::Restore => "restore",
            Self::Unreadable => "unreadable",
            Self::TaskDied => "task_died",
            Self::StorageFailed => "storage_failed",
            Self::StorageNotClosed => "storage_not_closed",
            Self::Unclean { .. } => "unclean",
        }
    }
}

/// What `kimmy.last-start` says about the previous start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastStart {
    pub at_ms: u64,
    /// The verdict that run started with: an exit name, `unclean`, `unreadable` or
    /// `first_start`.
    pub inherited: String,
    /// The build that wrote it.
    pub written_by: String,
}

#[derive(Clone, Debug)]
pub struct Inputs {
    pub end: PreviousEnd,
    /// Bytes available to the unprivileged user, and the filesystem's size.
    pub free_available: Option<u64>,
    pub fs_size: Option<u64>,
    pub last_start: Option<LastStart>,
    /// The sidecar's `written_by` as read before this start rewrites it.
    pub sidecar_written_by: Option<String>,
    pub now_ms: u64,
    /// The database's modification age for an unclean end. Said in the decision
    /// line; **never used to measure the run's length**.
    pub last_write_secs_ago: Option<u64>,
}

/// The decision, and the inputs it was made from, for the one `INFO` line every
/// start logs and for `kimmy_yield_probation`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub probation: bool,
    pub reasons: Vec<&'static str>,
    /// The previous run's length, or `None` when unknown.
    pub run_length: Option<Duration>,
    /// Whether `kimmy.last-start` was used (it matched the sidecar).
    pub last_start_used: bool,
}

/// `available < min(1 GiB, 5% of the filesystem)`. An unknown size or free count is
/// not low.
pub fn low_space(available: Option<u64>, fs_size: Option<u64>) -> bool {
    match (available, fs_size) {
        (Some(available), Some(size)) => available < LOW_SPACE_CAP.min(size / 20),
        _ => false,
    }
}

fn non_clean(name: &str) -> bool {
    matches!(name, "unclean" | "storage_not_closed" | "storage_failed" | "task_died")
}

pub fn decide(inputs: &Inputs) -> Decision {
    let usable = inputs
        .last_start
        .as_ref()
        .filter(|record| inputs.sidecar_written_by.as_deref() == Some(record.written_by.as_str()));
    let run_length = usable
        .and_then(|record| inputs.now_ms.checked_sub(record.at_ms).map(Duration::from_millis));
    let mut reasons = Vec::new();
    match &inputs.end {
        PreviousEnd::StorageFailed | PreviousEnd::TaskDied => reasons.push("previous run failed"),
        PreviousEnd::Unclean { .. } | PreviousEnd::StorageNotClosed => {
            if low_space(inputs.free_available, inputs.fs_size) {
                reasons.push("low free space");
            }
            if run_length.is_some_and(|length| length < SHORT_RUN) {
                reasons.push("short previous run");
            }
            let second = match &inputs.end {
                PreviousEnd::Unclean { before } => before.as_deref().is_some_and(non_clean),
                _ => false,
            } || usable.is_some_and(|record| non_clean(&record.inherited));
            if second {
                reasons.push("second non-clean end in a row");
            }
        }
        PreviousEnd::FirstStart
        | PreviousEnd::Shutdown
        | PreviousEnd::Error
        | PreviousEnd::Restore
        | PreviousEnd::Unreadable => {}
    }
    Decision {
        probation: !reasons.is_empty(),
        reasons,
        run_length,
        last_start_used: usable.is_some(),
    }
}
