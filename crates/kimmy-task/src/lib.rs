//! Supervision for the node's long-lived background tasks.
//!
//! # Why a task dying has to stop the process
//!
//! The release profile unwinds rather than aborting, and `start_and_serve` held
//! a `JoinHandle` for every background task without awaiting any of them until
//! shutdown. So a panicking task ended alone, with one plain-text line on
//! stderr outside the structured log, and stayed ended until someone restarted
//! the node. The worst of those is the session invalidator: while it is dead a
//! revoked token keeps working on this node until its TTL expires, which is a
//! security defect rather than an availability one.
//!
//! Exiting the process is the restart that works everywhere: compose consumes
//! no health signal, so failing liveness restarts nothing there (ADR-184 states
//! this, citing the same reasoning as the liveness work).
//!
//! **Not `panic = "abort"`.** That would make a panic anywhere fatal, including
//! in a request handler, which turns a crafted request into a remote kill
//! switch. Supervision is per task and by name, and the tasks that must *not*
//! be fatal — one per inbound connection, one per request — are simply not
//! supervised.
//!
//! # The three shapes
//!
//! - [`supervise`] — a loop that should never end. Any return is a death.
//! - [`Retry`] — the policy a supervised task uses inside its own loop when its
//!   errors are transient: counted, logged, backed off, and never returned.
//! - [`supervise_judged`] — a task with more than one way to end, which says
//!   which its return was. Guessing from outside is wrong when a task has a
//!   legitimate terminal condition.
//! - [`supervise_oneshot`] — work that ends on purpose, where **completion is
//!   expected and a panic is a death**. The membership timers are this: foca
//!   tolerates a delayed timer but not a lost one, so a panicking timer task
//!   freezes membership as surely as a dead receiver would.
//!
//! # Telling shutdown apart from a death
//!
//! Every helper takes a [`Shutdown`]. The shutdown path calls
//! [`Shutdown::begin`] **before** any step that could make a task return, and
//! each helper checks it twice: once as a `select!` branch, and again after the
//! task's own branch wins. The second check is the load-bearing one —
//! `tokio::select!` picks a ready branch at random, so without it a task that
//! returns at the moment shutdown begins is a coin flip between a clean stop
//! and a spurious process exit.

use std::future::Future;
use std::sync::OnceLock;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// How a supervised task ended, when it should not have.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Death {
    /// It unwound. The message is in the detail.
    Panicked,
    /// It returned, which for a task that should never end is a death of its
    /// own — and is indistinguishable from a normal return at the type level,
    /// since all but one of these tasks return `()`.
    Returned,
}

impl Death {
    /// The name this is recorded under, so an exit marker and a log line agree.
    pub fn name(self) -> &'static str {
        match self {
            Death::Panicked => "panicked",
            Death::Returned => "returned",
        }
    }
}

/// What the process does when a supervised task dies.
///
/// Injected rather than called directly, because the exit marker lives in the
/// daemon (`kimmyd::lifecycle`) and the tasks being supervised live in crates
/// below it. The daemon installs this once at startup, through
/// [`on_death`], next to where it installs the panic hook.
pub trait OnDeath: Send + Sync + 'static {
    /// Record the death and end the process. Never returns.
    fn exit(&self, task: &'static str, cause: Death, detail: &str) -> !;
}

static ON_DEATH: OnceLock<Box<dyn OnDeath>> = OnceLock::new();

/// Install the process's death behaviour. The first call wins.
///
/// Returns whether it was installed, so a second caller can be told rather
/// than silently ignored.
pub fn on_death(reporter: Box<dyn OnDeath>) -> bool {
    ON_DEATH.set(reporter).is_ok()
}

/// End the process because `task` died.
///
/// Public because the same exit path serves every restart-worthy state, not
/// only a dead task: the poisoned-engine detector is meant to call this rather
/// than grow an exit of its own.
/// The exit status for a restart-worthy state the process found in itself.
///
/// `EX_SOFTWARE`, and distinct from the 1 a configuration error gives. The
/// daemon uses the same number where it installs its own reporter.
pub const EXIT_RESTART_WORTHY: i32 = 70;

pub fn exit_because(task: &'static str, cause: Death, detail: &str) -> ! {
    match ON_DEATH.get() {
        Some(reporter) => reporter.exit(task, cause, detail),
        // Nothing installed: a test binary, or a build that forgets to install
        // one. It must still be loud and still stop, because a death that only
        // logs is the defect this module exists to remove.
        //
        // **On stderr as well as through `tracing`**, and that is not belt and
        // braces: a process with no subscriber installed -- which every test
        // binary is -- sends the `error!` nowhere, so the process vanished with
        // status 70 and no explanation at all. Found exactly that way, by a
        // membership test binary disappearing mid-run.
        None => {
            error!(
                task,
                cause = cause.name(),
                detail,
                "a supervised background task ended and no exit behaviour was installed"
            );
            eprintln!(
                "kimmy-task: the supervised task {task:?} {} ({detail}), and no exit behaviour \
                 was installed; stopping with {EXIT_RESTART_WORTHY}",
                cause.name()
            );
            std::process::exit(EXIT_RESTART_WORTHY);
        }
    }
}

/// Set once, before anything that could make a task return.
///
/// A clone shares the same state, so the shutdown path holds one and every
/// supervised task holds another.
#[derive(Clone, Debug)]
pub struct Shutdown(watch::Sender<bool>);

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    pub fn new() -> Self {
        Shutdown(watch::Sender::new(false))
    }

    /// Announce that shutdown has begun.
    ///
    /// **Call this before the first step that could make a supervised task
    /// return** — before aborting a handle, before closing a listener, before
    /// dropping anything a task is reading. Every helper's second check reads
    /// this, so a task that returns after it is a clean stop; one that returns
    /// before it is a death.
    pub fn begin(&self) {
        // `send_replace`, not `send`: `send` **fails and does not store the
        // value** when no receiver is currently subscribed, and a receiver only
        // exists while a task is inside `reached`. So `send` could return an
        // error nobody looked at and leave `has_begun` false, which would make
        // every "is this shutdown?" check answer no during shutdown -- turning
        // each abort into a death. Found by a test whose retry loop kept asking
        // to be retried after shutdown had begun.
        self.0.send_replace(true);
    }

    /// Whether [`begin`](Self::begin) has been called.
    pub fn has_begun(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves once shutdown has begun, immediately if it already has.
    async fn reached(&self) {
        let mut rx = self.0.subscribe();
        // `borrow_and_update` first, so a shutdown that began before this call
        // is not waited on for ever.
        if *rx.borrow_and_update() {
            return;
        }
        // The sender outlives every task it supervises; if it did not, a
        // dropped sender means the process is going away anyway.
        let _ = rx.changed().await;
    }
}

/// How many times each task's work has been retried, for
/// `kimmy_task_retries_total`.
///
/// A fixed, bounded label set: the tasks are named by `&'static str` literals
/// at their spawn sites, so this cannot grow without a code change.
static RETRIES: OnceLock<std::sync::Mutex<Vec<(&'static str, u64)>>> = OnceLock::new();

fn count_retry(task: &'static str) {
    let counts = RETRIES.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    let mut counts = counts.lock().expect("the retry counts are never held across a panic");
    match counts.iter_mut().find(|(name, _)| *name == task) {
        Some((_, n)) => *n += 1,
        None => counts.push((task, 1)),
    }
}

/// Every long-lived task the node supervises, by the name it is supervised
/// under.
///
/// A `const` here rather than a list registered at startup, for two reasons.
/// **`kimmy_task_retries_total` must be present from the first scrape**: the
/// exposition's standing rule is that no series is conditional, because one that
/// can be absent is one a dashboard can lose — and a registration would leave it
/// absent in any process that had not run startup, the `/metrics` render's own
/// tests included. And **it is what `every_supervised_task_is_declared` checks
/// the `supervise` calls against**, so a task added without a name here, or a
/// name here that nothing supervises, fails rather than drifting.
///
/// Three of these are spawned inside `membership::run` rather than by the
/// daemon: supervising that loop does not cover its children, because it goes on
/// running when they die.
pub const TASKS: &[&str] = &[
    "cert_reloader",
    "embedding_worker",
    "jwks_refresher",
    "membership",
    "membership_announce",
    "membership_inbound",
    "membership_timer",
    "replication",
    "replication_server",
    "retention_collector",
    "session_invalidator",
    "stall_probe",
    "ttl_expiry",
    "vector_index_invalidator",
    "webhook_dispatcher",
];

/// Every task in [`TASKS`] with the number of times its work has been retried.
///
/// Reported as `kimmy_task_retries_total{task}`. **Every task appears, at 0 if it
/// has never retried**, so the series is present from the first scrape. A count
/// that rises while nothing else changes is a task retrying for ever, which does
/// no work while looking alive — read it beside that task's progress age.
pub fn retries() -> Vec<(&'static str, u64)> {
    let observed =
        RETRIES.get().map(|c| c.lock().expect("not held across a panic").clone()).unwrap_or_default();
    let mut out: Vec<(&'static str, u64)> = TASKS
        .iter()
        .map(|task| {
            let n = observed.iter().find(|(name, _)| name == task).map_or(0, |(_, n)| *n);
            (*task, n)
        })
        .collect();
    out.sort_unstable_by_key(|(name, _)| *name);
    out
}

/// How `KIMMY_TEST_KILL_TASK` asks a supervised task to end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kill {
    /// Unwind, so the supervisor sees a panic.
    Panic,
    /// Return, so the supervisor sees a task that should not have returned.
    Return,
    /// Fail the work once, so a retrying task retries. Only a task with a retry
    /// loop reads this one.
    Error,
}

/// What `KIMMY_TEST_KILL_TASK` asks for, as `<task>:<panic|return|error>`.
///
/// **A test-only switch that is present in the shipped binary**, deliberately:
/// the tests that matter here drive a real `kimmyd` and assert its exit code and
/// the marker the next start reads, and a build-time feature would mean testing
/// a binary nobody ships. It is named in the `KIMMY_TEST_*` family, is never read
/// from the configuration file, is announced at `WARN` on every start where it is
/// set, and does nothing until [`arm_test_kills`] is called — which the daemon
/// does only once it is serving, so it cannot interfere with startup.
///
/// The environment is not remotely settable, so the exposure is a switch
/// available to whoever can already set the process's environment.
fn requested_kill() -> Option<(String, Kill)> {
    static PARSED: OnceLock<Option<(String, Kill)>> = OnceLock::new();
    PARSED
        .get_or_init(|| {
            let raw = std::env::var("KIMMY_TEST_KILL_TASK").ok()?;
            let (task, how) = raw.split_once(':')?;
            let how = match how {
                "panic" => Kill::Panic,
                "return" => Kill::Return,
                "error" => Kill::Error,
                _ => return None,
            };
            Some((task.to_string(), how))
        })
        .clone()
}

/// Whether `KIMMY_TEST_KILL_TASK` is set, as it was set, for the startup
/// warning. The raw value rather than a re-rendering of it, so an operator who
/// sees the line can match it against what is in their environment.
pub fn test_kill_requested() -> Option<String> {
    requested_kill().map(|_| std::env::var("KIMMY_TEST_KILL_TASK").unwrap_or_default())
}

/// How long after arming `KIMMY_TEST_KILL_TASK` waits, so the node is serving
/// first. Test-only, so a generous value costs nothing anyone ships.
const TEST_KILL_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

static ARMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Let `KIMMY_TEST_KILL_TASK` take effect from now on.
///
/// Called once the node is serving, so the switch cannot turn a startup into a
/// crash loop and cannot be confused with a startup failure.
pub fn arm_test_kills() {
    ARMED.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// What `KIMMY_TEST_KILL_TASK` asks of this task, once armed.
pub fn kill_for(task: &str) -> Option<Kill> {
    if !ARMED.load(std::sync::atomic::Ordering::SeqCst) {
        return None;
    }
    requested_kill().and_then(|(wanted, how)| (wanted == task).then_some(how))
}

/// Wait until this task is asked to end, if it ever is.
///
/// Polled rather than signalled: this exists only under test, and a poll costs a
/// wakeup a second in a process that is not being tested at all.
async fn awaiting_test_kill(task: &'static str) -> Kill {
    let how = loop {
        match kill_for(task) {
            Some(Kill::Panic) => break Kill::Panic,
            Some(Kill::Return) => break Kill::Return,
            // The retrying shape reads this itself; ending the task here would
            // test the wrong thing.
            Some(Kill::Error) | None => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };
    // The grace comes *after* the switch is seen, not before the loop: nothing
    // is armed when a task starts, so a grace there is skipped entirely and the
    // death races the node's first `/healthz`. A test cannot then assert the
    // order it cares about -- healthy, then a task died, then the process
    // exited -- and "after startup" has to mean after the node is actually up.
    tokio::time::sleep(TEST_KILL_GRACE).await;
    how
}

/// The work, plus the test switch, **inside** the supervised task.
///
/// Inside rather than racing it from the supervisor, so that what the supervisor
/// then classifies is a real panic in a real task — reaching it as a `JoinError`
/// carrying the message, exactly as a genuine one would. Racing it outside would
/// have panicked the supervisor instead, which nothing classifies and which
/// therefore would not have exited at all: the test would have proved the
/// opposite of what it claimed.
async fn with_test_kill<F: Future<Output = ()>>(name: &'static str, work: F) {
    tokio::select! {
        () = work => {}
        how = awaiting_test_kill(name) => match how {
            Kill::Panic => panic!("KIMMY_TEST_KILL_TASK asked {name} to panic"),
            // Returning here is a return of the task, which is what the
            // supervisor then judges.
            _ => {}
        },
    }
}

/// The detail line for a task that ended by unwinding.
fn panic_detail(e: tokio::task::JoinError) -> String {
    // The payload is the panic's own message when it was a `&str` or a
    // `String`, which covers `panic!`, `unwrap` and `expect`. Anything else is
    // reported as its absence rather than as an empty string, so a reader can
    // tell "no message" from "a message we could not read".
    match e.try_into_panic() {
        Ok(payload) => {
            if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "a panic payload of a type this build cannot render".to_string()
            }
        }
        Err(e) => e.to_string(),
    }
}

/// Supervise a task that should never end.
///
/// The work runs in a task of its own so that a panic arrives here as a
/// `JoinError` carrying its message, which is how the panic text reaches the
/// structured log rather than only stderr.
pub fn supervise<F>(name: &'static str, shutdown: Shutdown, work: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut running = tokio::spawn(with_test_kill(name, work));
        tokio::select! {
            _ = shutdown.reached() => {
                info!(task = name, "stopping a background task for shutdown");
                running.abort();
            }
            ended = &mut running => {
                // The second check. `select!` picks a ready branch at random,
                // so without this a task returning as shutdown begins is a coin
                // flip between a clean stop and a spurious exit.
                if shutdown.has_begun() {
                    info!(task = name, "a background task ended as shutdown began");
                    return;
                }
                match ended {
                    Ok(()) => exit_because(name, Death::Returned, "the task returned"),
                    Err(e) if e.is_panic() => {
                        exit_because(name, Death::Panicked, &panic_detail(e))
                    }
                    // Cancelled, which only our own `abort` does: a parent
                    // ending its children, or the shutdown path. A deliberate
                    // stop is not a death, and treating it as one made a task
                    // whose parent had finished kill the process -- which is how
                    // `membership_inbound` stopped a test binary with status 70
                    // when the loop it feeds returned.
                    // Cancelled, and *not* during shutdown -- the check above
                    // returned already if it were. Only our own `abort` cancels
                    // a handle, so this is a parent ending its children; ADR-184
                    // enumerates every such call site. Logged rather than
                    // passed over, because a stray `abort` on a supervised
                    // handle would otherwise be a death in costume.
                    Err(_) => {
                        info!(
                            task = name,
                            "a supervised background task was cancelled outside shutdown, so \
                             something aborted its handle deliberately"
                        );
                    }
                }
            }
        }
    })
}

/// The retry policy for a supervised task whose errors are transient, used from
/// inside the task's own loop.
///
/// The policy lives here — the counter, the log line and the backoff schedule —
/// and the loop stays at the call site. A higher-order version taking the work
/// as an async closure was tried first and rejected: the embedding worker's
/// `run` takes `&mut self`, so the closure's future borrows it, and proving that
/// future `Send` through a generic bound is not expressible on stable. A
/// two-line loop at the call site costs less than the abstraction did.
///
/// **Every error handed here is treated as transient.** A task whose failure is
/// permanent does not belong in this shape: ADR-184 hoists those two into
/// startup, so the node fails to start rather than retrying what cannot succeed.
pub struct Retry {
    task: &'static str,
    backoff: std::time::Duration,
    max: std::time::Duration,
}

impl Retry {
    pub fn new(
        task: &'static str,
        first_backoff: std::time::Duration,
        max_backoff: std::time::Duration,
    ) -> Self {
        Retry { task, backoff: first_backoff, max: max_backoff }
    }

    /// Count the failure, log it, and wait before the next attempt.
    ///
    /// Returns `false` once shutdown has begun, which is the caller's signal to
    /// return — and returning then is a clean stop, because [`supervise`] reads
    /// the same announcement.
    pub async fn after(&mut self, error: impl std::fmt::Display, shutdown: &Shutdown) -> bool {
        if shutdown.has_begun() {
            return false;
        }
        count_retry(self.task);
        warn!(
            task = self.task,
            error = %error,
            retry_in_ms = self.backoff.as_millis() as u64,
            "a background task failed and will retry in place"
        );
        tokio::time::sleep(self.backoff).await;
        self.backoff = (self.backoff * 2).min(self.max);
        !shutdown.has_begun()
    }
}

/// What a return meant, for a task that has more than one way to end.
///
/// `supervise` cannot tell an expected ending from a death when both are just a
/// return — and a task with a legitimate terminal condition is common enough
/// that guessing from outside is wrong. The membership receiver is the case:
/// it ends when the loop it feeds has gone, which is ordinary, and it ends on a
/// socket error, which is not. Only the task knows which.
#[derive(Debug)]
pub enum Ended {
    /// A terminal condition the task is written to reach. Logged, not fatal.
    Expected(&'static str),
    /// A return that should not have happened. Fatal, as for [`supervise`].
    Unexpected(&'static str),
}

/// Supervise a task that judges its own ending.
///
/// The same rules as [`supervise`] for a panic and for shutdown; the difference
/// is that a return is classified by the task rather than assumed to be a
/// death.
pub fn supervise_judged<F>(name: &'static str, shutdown: Shutdown, work: F) -> JoinHandle<()>
where
    F: Future<Output = Ended> + Send + 'static,
{
    tokio::spawn(async move {
        let mut running = tokio::spawn(work);
        tokio::select! {
            _ = shutdown.reached() => {
                info!(task = name, "stopping a background task for shutdown");
                running.abort();
            }
            ended = &mut running => {
                if shutdown.has_begun() {
                    info!(task = name, "a background task ended as shutdown began");
                    return;
                }
                match ended {
                    Ok(Ended::Expected(why)) => {
                        info!(task = name, reason = why, "a background task finished");
                    }
                    Ok(Ended::Unexpected(why)) => exit_because(name, Death::Returned, why),
                    Err(e) if e.is_panic() => {
                        exit_because(name, Death::Panicked, &panic_detail(e))
                    }
                    // Cancelled, and *not* during shutdown -- the check above
                    // returned already if it were. Only our own `abort` cancels
                    // a handle, so this is a parent ending its children; ADR-184
                    // enumerates every such call site. Logged rather than
                    // passed over, because a stray `abort` on a supervised
                    // handle would otherwise be a death in costume.
                    Err(_) => {
                        info!(
                            task = name,
                            "a supervised background task was cancelled outside shutdown, so \
                             something aborted its handle deliberately"
                        );
                    }
                }
            }
        }
    })
}

/// Supervise work that ends on purpose: completion is expected, a panic is a
/// death.
///
/// For the membership timers, which exist one per scheduled event. foca
/// tolerates a delayed timer but **not a lost one**, so a timer task that
/// unwinds freezes membership as surely as a dead receiver — while its
/// completion is the normal case and must not be read as a death.
pub fn supervise_oneshot<F>(name: &'static str, shutdown: Shutdown, work: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        match tokio::spawn(work).await {
            Ok(()) => {}
            Err(e) if e.is_panic() => {
                if shutdown.has_begun() {
                    return;
                }
                exit_because(name, Death::Panicked, &panic_detail(e));
            }
            // Cancelled. Only our own `abort` does that, and it is never
            // silent: see the note in `supervise`.
            Err(_) => {
                info!(task = name, "a one-shot background task was cancelled");
            }
        }
    })
}
