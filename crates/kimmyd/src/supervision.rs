//! What this process does when a supervised background task dies, and where a
//! panic's message goes.
//!
//! The policy lives here rather than in `kimmy-task` because the exit marker is
//! the daemon's (see [`crate::lifecycle`]), while the tasks being supervised are
//! spawned from crates below it. `kimmy_task::on_death` is the seam.

use std::path::PathBuf;

use tracing::error;

use crate::lifecycle;

/// Records the death in the exit marker and stops the process.
///
/// Exit code 70, `EX_SOFTWARE`, distinct from the 1 a configuration error
/// gives. Nothing consumes it — compose and Kubernetes restart on any non-zero
/// — so its only job is to tell an operator reading `docker inspect` that the
/// node stopped itself rather than failing to start.
struct ExitOnDeath {
    data_dir: PathBuf,
}

/// The exit code for a restart-worthy state this process detected in itself.
pub const RESTART_WORTHY: i32 = 70;

impl kimmy_task::OnDeath for ExitOnDeath {
    fn exit(&self, task: &'static str, cause: kimmy_task::Death, detail: &str) -> ! {
        // The log line first: the steps after it can fail, and this is the one
        // record that does not depend on the filesystem.
        error!(
            task,
            cause = cause.name(),
            detail,
            exit_code = RESTART_WORTHY,
            "a supervised background task ended; stopping the process so it is restarted"
        );
        lifecycle::record_task_death(&self.data_dir, task, &format!("{}: {detail}", cause.name()));
        // No destructors run, so the engine is not closed cleanly. redb repairs
        // an unclean file on the next open, and the marker written above is what
        // distinguishes this from a crash.
        std::process::exit(RESTART_WORTHY)
    }
}

/// Stop the process because the storage engine hit an I/O error (ADR-188).
///
/// Installed as the engine's one reaction, so it runs once, on the thread that
/// hit the error, possibly inside a write transaction. So it touches nothing
/// but the log and a plain file: the structured line, a line on stderr in case
/// no subscriber is installed, the exit marker, and a flush of both streams
/// before the exit, which runs no destructors.
fn exit_on_storage_failure(
    data_dir: &std::path::Path,
    failed: &kimmy_storage::health::StorageFailure,
) -> ! {
    use std::io::Write as _;
    let cause = format!("{}: {}", failed.call, failed.error);
    error!(
        call = failed.call,
        kind = ?failed.kind,
        error = %failed.error,
        exit_code = RESTART_WORTHY,
        "the storage engine hit an I/O error and serves nothing from here; stopping the \
         process so it is restarted, and the database is repaired when it reopens"
    );
    eprintln!(
        "kimmyd: the storage engine hit an I/O error ({cause}); stopping with {RESTART_WORTHY}"
    );
    lifecycle::record_storage_failure(data_dir, &cause);
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(RESTART_WORTHY)
}

/// Install the engine's reaction to a storage I/O error: stop the process.
pub fn install_storage_reaction(engine: &kimmy_storage::Engine, data_dir: PathBuf) -> bool {
    engine.on_storage_failure(Box::new(move |failed| exit_on_storage_failure(&data_dir, failed)))
}

/// Install the death behaviour and the panic hook. Call once, at startup.
///
/// Returns whether the death behaviour was installed; a second call is a
/// programming error rather than something to paper over.
pub fn install(data_dir: PathBuf) -> bool {
    install_panic_hook();
    kimmy_task::on_death(Box::new(ExitOnDeath { data_dir }))
}

/// Send a panic's message to the structured log as well as to stderr.
///
/// Needed **in addition to** the supervisor's own `JoinError`, not instead of
/// it. The supervisor sees a panic only in a task it supervises; this sees one
/// in a per-connection task, in `spawn_blocking`, and on the main thread — and
/// it is the only place with the location at panic time. The default hook is
/// kept and called after, so the backtrace an operator may have asked for with
/// `RUST_BACKTRACE` still appears.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        error!(panic = %info, "a thread panicked");
        default(info);
    }));
}
