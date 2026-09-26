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
//! # The four shapes
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

/// The exit status for a restart-worthy state the process found in itself.
///
/// `EX_SOFTWARE`, and distinct from the 1 a configuration error gives. The
/// daemon uses the same number where it installs its own reporter.
pub const EXIT_RESTART_WORTHY: i32 = 70;

/// The exit status for a shutdown that could not close its storage cleanly:
/// a write still in progress, or a last flush that failed, at the end of the
/// drain (ADR-192). `EX_TEMPFAIL`: distinct from 1, a configuration error,
/// and from 70, a failure found while serving. The run recorded no
/// clean-exit marker.
pub const EXIT_UNCLEAN_SHUTDOWN: i32 = 75;

/// How long a report before an exit may take before the exit happens without it.
pub const EXIT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// Exit [`EXIT_RESTART_WORTHY`] after [`EXIT_DEADLINE`], from a thread of its
/// own, whatever the caller is doing by then. Call it first thing on a way out.
///
/// `catch_unwind` bounds a report that panics, not one that blocks. A write to
/// stdout or stderr whose reader is alive but not reading waits for the pipe to
/// drain, and with it every other writer that wants the same lock: a log line
/// in the report, and the flushes after it, wait forever, and so does the exit.
/// So the exit is raw `_exit`, which neither flushes nor runs anything, and it
/// keeps the status 70 an operator reads, where `abort` would give a signal and
/// a core file. A thread that cannot be spawned leaves the exit unbounded, as
/// it was before.
pub fn exit_deadline() {
    // UNSUPERVISED: the exit path's own deadline. Supervising it would route its death
    // back into the exit it bounds.
    let _ = std::thread::Builder::new().name("exit-deadline".into()).spawn(|| {
        std::thread::sleep(EXIT_DEADLINE);
        #[cfg(unix)]
        // SAFETY: `_exit` takes a plain status and never returns; it touches no
        // Rust state, which is why it cannot block where `process::exit` can.
        unsafe {
            libc::_exit(EXIT_RESTART_WORTHY)
        };
        #[cfg(not(unix))]
        std::process::abort();
    });
}

/// End the process because `task` reached a restart-worthy state.
///
/// Public because the same exit path serves every such state, not only a dead
/// task: the poisoned-engine detector is meant to call this rather than grow an
/// exit of its own.
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
            exit_deadline();
            // Under `catch_unwind`, and with a write that ignores its error
            // rather than `eprintln!`, which panics when stderr cannot be
            // written: nothing in the report may stop the exit after it.
            let _ = std::panic::catch_unwind(|| {
                use std::io::Write as _;
                error!(
                    task,
                    cause = cause.name(),
                    detail,
                    "a supervised background task ended and no exit behaviour was installed"
                );
                let _ = writeln!(
                    std::io::stderr(),
                    "kimmy-task: the supervised task {task:?} {} ({detail}), and no exit \
                     behaviour was installed; stopping with {EXIT_RESTART_WORTHY}",
                    cause.name()
                );
            });
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
/// tests included. And **it is what
/// `every_supervised_name_is_in_the_task_list_and_every_entry_is_used` checks
/// the `supervise` calls against**, so a task added without a name here, or a
/// name here that nothing supervises, fails rather than drifting.
///
/// Three of these are spawned inside `membership::run` rather than by the
/// daemon: supervising that loop does not cover its children, because it goes on
/// running when they die.
/// The tasks that retry, so `KIMMY_TEST_KILL_TASK`'s `error` mode can act on
/// them.
///
/// **One of fifteen.** `:error` asks for a transient failure and a retry, which
/// only a task with a retry loop can honour — so for the other fourteen it does
/// nothing at all, and used to do it silently. `every_retrying_task_is_declared`
/// checks this against the `Retry::new` call sites, so it cannot drift.
pub const RETRYING: &[&str] = &["drop_purger", "embedding_worker"];

/// Every task name that has actually been supervised in this process.
///
/// Recorded so that a switch naming a task this node never started can say so
/// rather than looking like a switch that failed. A node with vectors disabled
/// starts no embedding worker, and `embedding_worker:panic` on it is a test that
/// waits for nothing.
static STARTED: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());

/// The task names supervised so far, in this process, each once.
pub fn started() -> Vec<&'static str> {
    STARTED.lock().expect("the started list is never held across a panic").clone()
}

/// Record that `name` has been supervised, once however often it is.
///
/// Called by each `supervise*` before it spawns, not from inside the spawned
/// supervisor: the node reads this list to decide which progress-age rows
/// `/metrics` carries, and it has to be complete before the HTTP listener binds,
/// which a push from a task that has not been polled yet does not promise.
///
/// **Once per name.** A membership timer is supervised once per scheduled event,
/// and pushing on every call grew this list by one entry per timer for the life
/// of the process.
fn mark_started(name: &'static str) {
    let mut started = STARTED.lock().expect("the started list is never held across a panic");
    if !started.contains(&name) {
        started.push(name);
    }
}

pub const TASKS: &[&str] = &[
    "cert_reloader",
    "drop_purger",
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
    let observed = RETRIES
        .get()
        .map(|c| c.lock().expect("not held across a panic").clone())
        .unwrap_or_default();
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
    // Whenever it is **set**, not only when it parses. A value with a typo in
    // it used to be announced by nothing at all: the parse failed, this
    // returned `None`, and a test that thought it had armed a kill watched a
    // node shut down normally and had to work out why. A switch that does
    // nothing has to say so.
    let raw = std::env::var("KIMMY_TEST_KILL_TASK").ok()?;
    Some(match requested_kill() {
        None => format!(
            "{raw} -- malformed, so nothing will happen; expected <task>:<panic|return|error>"
        ),
        Some((task, _)) if !TASKS.contains(&task.as_str()) => format!(
            "{raw} -- matches no task, so nothing will happen; the names are kimmy_task::TASKS"
        ),
        // `error` asks for a transient failure and a retry, which only a task
        // with a retry loop can honour.
        Some((task, Kill::Error)) if !RETRYING.contains(&task.as_str()) => format!(
            "{raw} -- error mode only acts on a retrying task, so nothing will happen; the \
             retrying tasks are kimmy_task::RETRYING"
        ),
        Some(_) => raw,
    })
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
    // Startup is over, so what this node runs is now known. A switch naming a
    // task that never started here would otherwise wait for ever and look like a
    // switch that failed: a node with `vector.worker_enabled = false` starts no
    // embedding worker, and a single node starts none of the membership tasks.
    if let Some((task, _)) = requested_kill()
        && TASKS.contains(&task.as_str())
        && !started().contains(&task.as_str())
    {
        warn!(
            KIMMY_TEST_KILL_TASK = %task,
            started = ?started(),
            "the test switch names a task this node did not start, so nothing will happen; it is \
             not configured on this node"
        );
    }
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
    // **Decided once, and only by the task it names.** The switch comes from
    // the environment and cannot change while the process runs, so every other
    // task can settle this at startup and never wake again. It used to poll
    // every 50ms, for every supervised task, for the life of a node that was
    // not being tested at all -- fifteen timers, three hundred wakeups a
    // second, shipped.
    let Some((wanted, how)) = requested_kill() else { return never().await };
    if wanted != task {
        return never().await;
    }
    // The retrying shape reads this one itself; ending the task here would test
    // the wrong thing.
    if how == Kill::Error {
        return never().await;
    }
    // `ARMED` is the one thing that still has to be waited for: tasks start
    // before the node serves, and the switch must not fire during startup. A
    // poll here costs nothing, because only the named task reaches it.
    while !ARMED.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // The grace comes *after* the switch is seen, not before the loop: nothing
    // is armed when a task starts, so a grace there is skipped entirely and the
    // death races the node's first `/healthz`. A test cannot then assert the
    // order it cares about -- healthy, then a task died, then the process
    // exited -- and "after startup" has to mean after the node is actually up.
    tokio::time::sleep(TEST_KILL_GRACE).await;
    how
}

/// Wait until this task is asked to fail once, if it ever is.
///
/// The `Kill::Error` counterpart of [`awaiting_test_kill`], and separate because
/// the two end differently: that one ends the task, this one makes one attempt
/// fail so the retry is the thing under test.
async fn awaiting_test_error(task: &'static str) {
    let Some((wanted, Kill::Error)) = requested_kill() else { return never().await };
    if wanted != task {
        return never().await;
    }
    while !ARMED.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    tokio::time::sleep(TEST_KILL_GRACE).await;
}

/// A future that never resolves, for a task the switch does not name.
async fn never<T>() -> T {
    std::future::pending().await
}

/// The work, plus the test switch, **inside** the supervised task.
///
/// Inside rather than racing it from the supervisor, so that what the supervisor
/// then classifies is a real panic in a real task — reaching it as a `JoinError`
/// carrying the message, exactly as a genuine one would. Racing it outside would
/// have panicked the supervisor instead, which nothing classifies and which
/// therefore would not have exited at all: the test would have proved the
/// opposite of what it claimed.
async fn with_test_kill<T, F: Future<Output = T>>(name: &'static str, work: F) -> Result<T, Kill> {
    tokio::select! {
        v = work => Ok(v),
        how = awaiting_test_kill(name) => {
            if how == Kill::Panic {
                panic!("KIMMY_TEST_KILL_TASK asked {name} to panic");
            }
            // A `Err(kill)` rather than a value: a switch-induced return must be
            // a death even for a task whose own returns are expected, or
            // `supervise_oneshot` and `supervise_judged` would swallow it and
            // the switch would do nothing the WARN promised.
            Err(how)
        }
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

/// Stop the work when the supervisor stops.
///
/// **The reason this type exists.** A supervised handle is the *supervisor's*
/// handle, and the node's drain aborts ten of them. Aborting the supervisor
/// dropped the inner `JoinHandle`, and dropping a `JoinHandle` does not cancel
/// the task — it **detaches** it. So every shutdown left the real work running,
/// unsupervised, until the runtime went away; a panic in it after that point
/// was nobody's, and the process could end with status 0 and no report. An
/// independent review measured it on real SIGTERMs: 5 of 10 runs had
/// supervisors that never logged "stopping a background task for shutdown",
/// because they had been aborted before they could.
struct StopOnDrop<T> {
    task: &'static str,
    handle: JoinHandle<T>,
    shutdown: Shutdown,
}

impl<T> Drop for StopOnDrop<T> {
    fn drop(&mut self) {
        // Finished work needs no aborting, and saying anything about it would
        // make the line below fire on every ordinary ending.
        if self.handle.is_finished() {
            return;
        }
        self.handle.abort();
        if !self.shutdown.has_begun() {
            // Outside shutdown this means something aborted a supervised handle
            // deliberately. That is now honest — the work really stops — but it
            // is still a task disappearing without a death, so it is said out
            // loud rather than inferred from its silence.
            info!(
                task = self.task,
                "a supervised task's handle was dropped outside shutdown, so its work was \
                 stopped with it"
            );
        }
    }
}

/// What a supervised task's return means to its supervisor.
enum Return {
    /// Nothing to say: the work was meant to finish.
    Expected,
    /// Finished for a reason worth logging.
    Finished(&'static str),
    /// A return that should not have happened.
    Fatal(Death, String),
}

/// The one place a supervised ending is classified.
///
/// Shared by all three supervisors so that the shutdown re-check exists **once**
/// and one test covers every shape. It was written out three times, and only
/// `supervise`'s copy was ever tested: two of the three could have lost the
/// re-check without anything failing.
fn classify<T>(
    name: &'static str,
    shutdown: &Shutdown,
    ended: Result<Result<T, Kill>, tokio::task::JoinError>,
    judge: impl FnOnce(T) -> Return,
) {
    // The second check. `select!` picks a ready branch at random, so without
    // this a task returning as shutdown begins is a coin flip between a clean
    // stop and a spurious exit.
    if shutdown.has_begun() {
        info!(task = name, "a background task ended as shutdown began");
        return;
    }
    match ended {
        Ok(Ok(value)) => match judge(value) {
            Return::Expected => {}
            Return::Finished(why) => {
                info!(task = name, reason = why, "a background task finished");
            }
            Return::Fatal(cause, detail) => exit_because(name, cause, &detail),
        },
        // The test switch asked for it, and it is a death whatever this task's
        // own returns mean.
        Ok(Err(_)) => {
            exit_because(name, Death::Returned, "KIMMY_TEST_KILL_TASK asked this task to return")
        }
        Err(e) if e.is_panic() => exit_because(name, Death::Panicked, &panic_detail(e)),
        // Cancelled: the inner handle. Only `StopOnDrop` aborts it, and that
        // path does not await the handle afterwards, so this stays hard to
        // reach — kept because folding it in with a clean return would make a
        // future stray abort a silent exit 70.
        Err(_) => {
            info!(
                task = name,
                "a supervised background task was cancelled outside shutdown, so something \
                 aborted its handle deliberately"
            );
        }
    }
}

/// The body every supervisor shares: run the work in a task of its own, stop it
/// if this supervisor goes away, and classify how it ended.
///
/// The work runs in a task of its own so that a panic arrives here as a
/// `JoinError` carrying its message, which is how the panic text reaches the
/// structured log rather than only stderr.
async fn supervised<T, F, J>(name: &'static str, shutdown: Shutdown, work: F, judge: J)
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
    J: FnOnce(T) -> Return,
{
    let mut running = StopOnDrop {
        task: name,
        // UNSUPERVISED: the supervised task itself. This is the spawn every other one goes
        // through, and `StopOnDrop` plus `classify` below are its supervision.
        handle: tokio::spawn(with_test_kill(name, work)),
        shutdown: shutdown.clone(),
    };
    tokio::select! {
        _ = shutdown.reached() => {
            info!(task = name, "stopping a background task for shutdown");
            // `running` drops here and stops the work.
        }
        ended = &mut running.handle => classify(name, &shutdown, ended, judge),
    }
}

/// Supervise a task that should never end: any return is a death.
pub fn supervise<F>(name: &'static str, shutdown: Shutdown, work: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    mark_started(name);
    // UNSUPERVISED: the supervisor task. Its body is `supervised`, which is the supervision.
    tokio::spawn(supervised(name, shutdown, work, |()| {
        Return::Fatal(Death::Returned, "the task returned".into())
    }))
}

/// The retry policy for a supervised task whose errors are transient, used from
/// inside the task's own loop.
///
/// The policy lives here, and so does the loop, in [`Retry::forever`]. It was at
/// the call site first, on the grounds that two lines cost less than the
/// abstraction — and the whole workspace suite then passed with that loop's
/// `Err` arm returning instead of retrying, because no test reaches a call site.
/// The `AsyncFnMut` form really is not expressible on stable (the worker's `run`
/// takes `&mut self`, so the returned future borrows it and no generic bound
/// proves it `Send`); a higher-ranked bound over a boxed future is, at one
/// allocation per failure.
///
/// **Every error handed here is treated as transient.** A task whose failure is
/// permanent does not belong in this shape: ADR-184 hoists those two into
/// startup, so the node fails to start rather than retrying what cannot succeed.
pub struct Retry {
    task: &'static str,
    backoff: std::time::Duration,
    first: std::time::Duration,
    max: std::time::Duration,
    /// When the last failure was handed here, so a quiet stretch can reset the
    /// backoff.
    last_failure: Option<tokio::time::Instant>,
}

/// How long a task must go without failing before its backoff starts over, as a
/// multiple of the wait it had reached.
///
/// **Without this the backoff never came down.** It doubles to the maximum and
/// stays there for the life of the process, so a task that failed a few times
/// on Monday waits the full two minutes for an unrelated transient error on
/// Friday — and the error that recovers on the first retry is exactly the error
/// most likely to be waited out for two minutes for no reason.
///
/// Time rather than a successful attempt, because `Retry` never sees success:
/// the only task that retries is the embedding worker, whose `run` does not
/// return `Ok` while it is working. What it can see is that nothing has failed
/// for a while, which is the same information from the other side.
///
/// Four, so the stretch is comfortably longer than the wait it is judging —
/// a task erroring every backoff period is still in trouble and keeps its long
/// wait, while one that has been quiet for four of them has recovered.
const BACKOFF_RESET_AFTER: u32 = 4;

impl Retry {
    pub fn new(
        task: &'static str,
        first_backoff: std::time::Duration,
        max_backoff: std::time::Duration,
    ) -> Self {
        Retry {
            task,
            backoff: first_backoff,
            first: first_backoff,
            max: max_backoff,
            last_failure: None,
        }
    }

    /// Run `step` until it succeeds or shutdown begins, retrying every error.
    ///
    /// **This exists because the rule is not testable at the call site.** With
    /// the loop written out in `node.rs`, the whole workspace suite passed with
    /// its `Err` arm returning instead of retrying — measured, not assumed — so
    /// the one rule this type is for had no test anywhere. Here a fake worker
    /// reaches it.
    ///
    /// The `AsyncFnMut` form really is not expressible: the worker's `run` takes
    /// `&mut self`, so the returned future borrows it, and no generic bound on
    /// stable proves that future `Send`. A higher-ranked bound over a boxed
    /// future does, at one allocation per attempt — which is per *failure*, so
    /// it costs nothing on the path that matters.
    pub async fn forever<W, E>(
        &mut self,
        shutdown: &Shutdown,
        worker: &mut W,
        mut step: impl for<'a> FnMut(
            &'a mut W,
        ) -> std::pin::Pin<
            Box<dyn Future<Output = Result<(), E>> + Send + 'a>,
        >,
    ) where
        E: std::fmt::Display,
    {
        // The test switch's third shape, which only a retrying task can honour:
        // fail the work once and let the retry happen. `with_test_kill`
        // deliberately leaves `Kill::Error` alone, because ending the task
        // there would test the opposite of the rule this loop exists for.
        //
        // **Raced against the step, not checked between steps.** Checking at
        // the top of the loop was the first version and it did nothing at all
        // for the one task that retries: the embedding worker's `run` does not
        // return while it is working, so the loop never came back round to
        // look. `KIMMY_TEST_KILL_TASK=embedding_worker:error` announced itself
        // at WARN on every start and then had no effect whatsoever.
        //
        // Once, as `Kill::Error`'s own documentation says: the point is that a
        // failure is retried and the task lives on, so a switch that failed
        // every attempt for ever would prove the task never works again.
        let mut injected = false;
        loop {
            let outcome: Result<(), String> = if injected {
                step(worker).await.map_err(|e| e.to_string())
            } else {
                tokio::select! {
                    v = step(worker) => v.map_err(|e| e.to_string()),
                    // **This cancels the attempt rather than making the work
                    // return an error of its own**, so what it exercises is this
                    // loop's `Err` arm -- the retry, the count, the backoff --
                    // and not the worker's own error path. Worth saying, because
                    // a test that read it the other way would think it had
                    // covered the worker.
                    () = awaiting_test_error(self.task) => {
                        injected = true;
                        Err("KIMMY_TEST_KILL_TASK asked for one failure".to_string())
                    }
                }
            };
            match outcome {
                // Success means the work finished, and this work should not:
                // returning hands that judgement to `supervise`, which calls a
                // return a death.
                Ok(()) => return,
                Err(e) => {
                    if !self.after(e, shutdown).await {
                        return;
                    }
                }
            }
        }
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
        // A quiet stretch starts the schedule over (see `BACKOFF_RESET_AFTER`).
        let now = tokio::time::Instant::now();
        if let Some(last) = self.last_failure
            && now.duration_since(last) > self.backoff * BACKOFF_RESET_AFTER
            && self.backoff > self.first
        {
            info!(
                task = self.task,
                quiet_secs = now.duration_since(last).as_secs(),
                "a background task has been failing again after a quiet stretch, so its retry \
                 backoff starts over rather than staying at the maximum it had reached"
            );
            self.backoff = self.first;
        }
        self.last_failure = Some(now);
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
    mark_started(name);
    // UNSUPERVISED: the supervisor task, as in `supervise`.
    tokio::spawn(supervised(name, shutdown, work, |ended| match ended {
        Ended::Expected(why) => Return::Finished(why),
        Ended::Unexpected(why) => Return::Fatal(Death::Returned, why.into()),
    }))
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
    mark_started(name);
    // UNSUPERVISED: the supervisor task, as in `supervise`.
    tokio::spawn(supervised(name, shutdown, work, |()| Return::Expected))
}
