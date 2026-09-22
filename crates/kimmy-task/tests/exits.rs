//! A supervised death stops the process even when nothing has been installed to
//! record it.
//!
//! These re-run this test binary as a child, because the thing being asserted is
//! an exit status: a test that called the classifier and checked what it
//! *decided* would prove the decision and nothing about the process stopping,
//! which is the whole point. The child is selected by `KIMMY_TASK_PROBE` and
//! runs exactly one scenario.
//!
//! **The no-reporter path is the one to be careful about.** A binary that has not
//! installed an `OnDeath` — a test harness, or a second binary added later — must
//! still be loud and still stop. Silence there is the shape this crate exists to
//! remove, and it is not hypothetical: while building this, a test binary was
//! exiting 70 with no explanation at all, because the only report went through
//! `tracing` and a test binary installs no subscriber.

use std::process::Command;
use std::time::Duration;

/// Run this binary again, as a child, with one scenario selected.
fn probe(scenario: &str, test_name: &str) -> std::process::Output {
    probe_with(scenario, test_name, &[])
}

/// [`probe`], with extra environment — for `KIMMY_TEST_KILL_TASK`, which is read
/// once per process, so each scenario that uses it needs a process of its own.
fn probe_with(scenario: &str, test_name: &str, env: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(std::env::current_exe().expect("this test binary"));
    command.args(["--exact", test_name, "--nocapture"]).env("KIMMY_TASK_PROBE", scenario);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("re-running this test binary")
}

fn scenario() -> Option<String> {
    std::env::var("KIMMY_TASK_PROBE").ok()
}

#[test]
fn a_panicking_task_exits_70_with_no_reporter_installed() {
    if scenario().as_deref() == Some("panic") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            kimmy_task::supervise("probe_task", kimmy_task::Shutdown::new(), async {
                panic!("the probe panics on purpose");
            });
            // Long enough for the supervisor to classify and exit; if it does
            // not, the child ends 0 and the parent's assertion says so.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        return;
    }

    let out = probe("panic", "a_panicking_task_exits_70_with_no_reporter_installed");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(70),
        "a panicking supervised task must stop the process even with no reporter installed; \
         stderr: {stderr}"
    );
    assert!(
        stderr.contains("no exit behaviour was installed"),
        "the default path must say why, on stderr, because a process with no tracing subscriber \
         sends the structured line nowhere: {stderr}"
    );
    assert!(stderr.contains("probe_task"), "and name the task: {stderr}");
    assert!(stderr.contains("panicked"), "and how it died: {stderr}");
    // And the panic's own words, *in the report's own sentence*. Asserting only
    // that stderr contains the message would prove nothing: Rust's default
    // panic hook prints it there itself, so such an assertion passes with the
    // payload thrown away. Written that looser way first, and it survived the
    // mutation that makes `panic_detail` return an empty string -- a test that
    // could not see the thing it was added for. The whole phrase can only come
    // from the line above.
    assert!(
        stderr
            .contains(r#"the supervised task "probe_task" panicked (the probe panics on purpose)"#),
        "the report must carry the panic's own message, not just the task's name: reading the \
         payload off the JoinError is the only reason the work runs in a task of its own, and an \
         operator with the name but not the message has nothing to act on: {stderr}"
    );
}

#[test]
fn a_task_that_judges_its_own_return_unexpected_exits_70() {
    if scenario().as_deref() == Some("judged") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            kimmy_task::supervise_judged("probe_judged", kimmy_task::Shutdown::new(), async {
                kimmy_task::Ended::Unexpected("the probe calls its own return a death")
            });
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        return;
    }

    let out = probe("judged", "a_task_that_judges_its_own_return_unexpected_exits_70");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(70), "stderr: {stderr}");
    assert!(stderr.contains("probe_judged"), "{stderr}");
    assert!(
        stderr.contains("the probe calls its own return a death"),
        "the reason is the task's own words: {stderr}"
    );
}

#[test]
fn a_task_that_judges_its_own_return_expected_does_not_exit() {
    // The other arm of the judged shape, and the one that matters: a task with a
    // legitimate terminal condition must be able to reach it without stopping
    // the node. The membership receiver is the one that does: it ends when the
    // loop it feeds has gone, which is ordinary.
    if scenario().as_deref() == Some("expected") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            kimmy_task::supervise_judged("probe_expected", kimmy_task::Shutdown::new(), async {
                kimmy_task::Ended::Expected("the probe finishes on purpose")
            });
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        return;
    }

    let out = probe("expected", "a_task_that_judges_its_own_return_expected_does_not_exit");
    assert_eq!(
        out.status.code(),
        Some(0),
        "an expected ending is not a death; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_task_stopped_for_shutdown_does_not_exit() {
    // Shutdown aborts supervised tasks, and this is that path: begin, then
    // abort, then a clean exit.
    //
    // **It is not the control for the announcement, though it was written as
    // one.** `abort()` cancels the *supervisor*, so nothing is left to classify
    // the ending and the exit-0 here holds however the shutdown checks behave —
    // blinding all three of them leaves this green. What it does cover is that
    // the ordinary drain path is quiet. The control is
    // `a_task_that_ends_by_itself_during_shutdown_does_not_exit` below.
    if scenario().as_deref() == Some("shutdown") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let shutdown = kimmy_task::Shutdown::new();
            let handle = kimmy_task::supervise("probe_shutdown", shutdown.clone(), async {
                std::future::pending::<()>().await;
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
            shutdown.begin();
            tokio::time::sleep(Duration::from_millis(500)).await;
            handle.abort();
            tokio::time::sleep(Duration::from_millis(500)).await;
        });
        return;
    }

    let out = probe("shutdown", "a_task_stopped_for_shutdown_does_not_exit");
    assert_eq!(
        out.status.code(),
        Some(0),
        "a task stopped for shutdown is not a death; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The work for the announcement's two arms: a task that ends of its own accord
/// once shutdown has begun, which is what a task written to notice a drain does.
///
/// `reached` is private, so it watches `has_begun` instead; the point is only
/// that the *work returns*, not how it waited.
async fn ends_when_shutdown_begins(shutdown: kimmy_task::Shutdown) {
    // A tight yield loop rather than a sleep, so the work returns in the same
    // scheduler tick that shutdown begins. That is the point: both of the
    // supervisor's `select!` branches become ready together, which is the race
    // the second check exists for.
    while !shutdown.has_begun() {
        tokio::task::yield_now().await;
    }
}

#[test]
fn a_task_that_ends_by_itself_during_shutdown_does_not_exit() {
    // The real control for the announcement, and a different shape from
    // `a_task_stopped_for_shutdown_does_not_exit` above -- which, on its own,
    // cannot see it. `abort()` cancels the *supervisor*, so nothing classifies
    // anything and the exit-0 there holds however the shutdown checks behave.
    // Verified: blinding all three of them leaves that test, and the end-to-end
    // drain test, green.
    //
    // Here the work returns by itself *as* shutdown begins. A return is a death
    // unless the announcement is read, so this is the one shape that fails when
    // it is not.
    //
    // **Repeated, because the thing under test is a race.** `select!` picks
    // among ready branches at random, so a single run is a coin flip between the
    // shutdown branch (which returns before classifying anything, and so proves
    // nothing) and the ended branch, where the check lives. One round is
    // therefore a 50% test. Thirty rounds in one process, with the first spurious
    // exit ending it, leaves a broken check about one chance in a billion of
    // passing -- and a first version of this test used a 20ms sleep instead of a
    // yield, which let the shutdown branch win every time and stayed green with
    // the check blinded.
    const ROUNDS: usize = 30;
    if scenario().as_deref() == Some("ended_during_shutdown") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            for _ in 0..ROUNDS {
                let shutdown = kimmy_task::Shutdown::new();
                kimmy_task::supervise(
                    "probe_drain",
                    shutdown.clone(),
                    ends_when_shutdown_begins(shutdown.clone()),
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
                shutdown.begin();
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        });
        return;
    }

    let out =
        probe("ended_during_shutdown", "a_task_that_ends_by_itself_during_shutdown_does_not_exit");
    assert_eq!(
        out.status.code(),
        Some(0),
        "a task that finished because the node is stopping is not a death; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_same_task_ending_with_no_shutdown_announced_does_exit() {
    // And the other arm, which is what makes the one above mean anything: the
    // identical work, returning for the identical reason, with nothing
    // announced. Without this pair an exit-0 could come from the supervisor
    // never classifying at all -- which is precisely the hole the test above
    // was written to fill.
    if scenario().as_deref() == Some("ended_no_shutdown") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let watched = kimmy_task::Shutdown::new();
            // The supervisor's own announcement, never begun; the work watches a
            // second one, which is begun. So the work ends for the same reason
            // and the supervisor has heard nothing.
            let ending = kimmy_task::Shutdown::new();
            kimmy_task::supervise(
                "probe_drain",
                watched,
                ends_when_shutdown_begins(ending.clone()),
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
            ending.begin();
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        return;
    }

    let out =
        probe("ended_no_shutdown", "the_same_task_ending_with_no_shutdown_announced_does_exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(70),
        "with no shutdown announced the same return is a death, which is what makes the previous \
         test a control rather than a coincidence; stderr: {stderr}"
    );
    assert!(stderr.contains("probe_drain"), "{stderr}");
}

#[tokio::test]
async fn aborting_a_supervised_handle_stops_the_work() {
    // The node's drain aborts every supervised handle. That handle is the
    // *supervisor*, and aborting it drops the inner `JoinHandle` -- which does
    // not cancel the inner task, it detaches it. So the work kept running,
    // unsupervised, until the runtime went away: a panic in it after that point
    // was nobody's, and the process ended 0 with no report.
    //
    // Measured by an independent review of this PR on real SIGTERMs: 5 of 10
    // runs had supervisors that never logged "stopping a background task for
    // shutdown" because they had been aborted before they could.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let ticks = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ticks);
    let shutdown = kimmy_task::Shutdown::new();
    let handle = kimmy_task::supervise("probe_abort", shutdown, async move {
        loop {
            counted.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(60)).await;
    let before = ticks.load(Ordering::Relaxed);
    assert!(before > 5, "premise: the work is running ({before} ticks)");

    handle.abort();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let after = ticks.load(Ordering::Relaxed);
    assert!(
        after <= before + 1,
        "aborting a supervised handle must stop the work, not detach it: {before} ticks at the \
         abort, {after} after 150ms more"
    );
}

#[test]
fn a_handle_dropped_outside_shutdown_says_so() {
    // The other half of the guard. Stopping the work silently would be an
    // improvement on detaching it silently and still leave a supervised task
    // disappearing with nothing said, so the notice is asserted rather than
    // assumed -- and it is the only observable effect the guard has, which is
    // why this test installs a subscriber where the others read an exit status.
    if scenario().as_deref() == Some("dropped") {
        tracing_subscriber::fmt().with_writer(std::io::stderr).with_ansi(false).init();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle =
                kimmy_task::supervise("probe_dropped", kimmy_task::Shutdown::new(), async {
                    std::future::pending::<()>().await;
                });
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.abort();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        return;
    }

    let out = probe("dropped", "a_handle_dropped_outside_shutdown_says_so");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "a deliberate abort is not a death: {stderr}");
    assert!(
        stderr.contains("dropped outside shutdown"),
        "an abort outside shutdown is said out loud, not inferred from silence: {stderr}"
    );
    assert!(stderr.contains("probe_dropped"), "and it names the task: {stderr}");
}

#[test]
fn the_test_switch_reaches_a_judged_task_and_a_one_shot() {
    // `KIMMY_TEST_KILL_TASK` only ever wrapped `supervise`. `supervise_judged`
    // and `supervise_oneshot` did not go through `with_test_kill` at all, so
    // `membership_inbound:panic`, `membership_timer:panic` and
    // `membership_announce:return` announced themselves at WARN and then did
    // nothing — three of the fifteen names the WARN offers, silently inert.
    //
    // Both shapes here, under the names they are used with, because the two
    // failed for the same reason and a fix to one is no evidence about the
    // other. A switch-induced return is a death even for a shape whose own
    // returns are expected, or the one-shot would swallow it.
    for (scenario, task, how, shape) in [
        ("kill_judged", "membership_inbound", "panic", "supervise_judged"),
        ("kill_oneshot", "membership_timer", "return", "supervise_oneshot"),
    ] {
        if scenario_is(scenario) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                kimmy_task::arm_test_kills();
                if scenario == "kill_judged" {
                    kimmy_task::supervise_judged(
                        "membership_inbound",
                        kimmy_task::Shutdown::new(),
                        async { std::future::pending::<kimmy_task::Ended>().await },
                    );
                } else {
                    kimmy_task::supervise_oneshot(
                        "membership_timer",
                        kimmy_task::Shutdown::new(),
                        async { std::future::pending::<()>().await },
                    );
                }
                // The grace after arming, and then some.
                tokio::time::sleep(Duration::from_secs(8)).await;
            });
            return;
        }

        let out = probe_with(
            scenario,
            "the_test_switch_reaches_a_judged_task_and_a_one_shot",
            &[("KIMMY_TEST_KILL_TASK", &format!("{task}:{how}"))],
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            out.status.code(),
            Some(70),
            "{shape} must honour the switch the WARN offers ({task}:{how}); stderr: {stderr}"
        );
        assert!(stderr.contains(task), "and name the task: {stderr}");
    }
}

fn scenario_is(want: &str) -> bool {
    scenario().as_deref() == Some(want)
}

#[tokio::test]
async fn a_transient_failure_is_retried_in_place_and_counted() {
    // The third class, and the one with no exit to assert: a transient `Err` is
    // retried for ever and never returned, so the task stays alive and
    // `kimmy_task_retries_total` is what makes the retrying visible.
    //
    // At this level rather than through a real `kimmyd`, because the only
    // retrying task in the node is the embedding worker, which needs a
    // configured provider to run at all — so an end-to-end arm would be a test
    // of vector configuration wearing this claim's name.
    let shutdown = kimmy_task::Shutdown::new();
    let mut retry = kimmy_task::Retry::new(
        "embedding_worker",
        Duration::from_millis(1),
        Duration::from_millis(4),
    );
    let before = count_for("embedding_worker");
    for _ in 0..3 {
        assert!(
            retry.after("a transient storage failure", &shutdown).await,
            "a transient failure asks the caller to carry on, not to return"
        );
    }
    assert_eq!(
        count_for("embedding_worker") - before,
        3,
        "each retry is counted, so a task retrying for ever is visible rather than silent"
    );

    // And once shutdown has begun it stops asking, so a retrying task ends
    // cleanly instead of retrying through a drain.
    shutdown.begin();
    assert!(!retry.after("a failure during shutdown", &shutdown).await);
}

#[tokio::test]
async fn a_failing_worker_is_retried_rather_than_abandoned() {
    // The rule the embedding worker lives by, at the only level where a test can
    // reach it. Before `Retry::forever` existed the loop was written out in
    // `node.rs`, and the entire workspace suite passed with its `Err` arm
    // returning instead of retrying: the rule was documented, reviewed, and
    // guarded by nothing.
    //
    // A fake worker rather than a real one, because the real one needs a
    // configured provider, and an arm that needed that would be a test of vector
    // configuration wearing this claim's name.
    struct FlakyWorker {
        attempts: usize,
    }
    impl FlakyWorker {
        async fn run(&mut self) -> Result<(), String> {
            self.attempts += 1;
            // Never succeeds. A worker that keeps failing must keep being
            // retried, which is the whole point: the node stays up and the
            // retry counter is what makes the trouble visible.
            Err(format!("attempt {} failed", self.attempts))
        }
    }

    let shutdown = kimmy_task::Shutdown::new();
    // A different task's name from the test above, deliberately: the retry
    // counts are process-wide, that test asserts an exact `+3` on
    // `embedding_worker`, and two tests in one binary sharing a counter would
    // make each one's result depend on the other's timing. The name is
    // incidental to what is being checked here.
    let mut retry =
        kimmy_task::Retry::new("ttl_expiry", Duration::from_millis(1), Duration::from_millis(2));
    let mut worker = FlakyWorker { attempts: 0 };
    let before = count_for("ttl_expiry");

    // Stop it from outside after a moment, which is the only way a `forever`
    // that is behaving ends.
    let stopper = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(120)).await;
        stopper.begin();
    });
    retry.forever(&shutdown, &mut worker, |w| Box::pin(w.run())).await;

    assert!(
        worker.attempts > 3,
        "a worker whose every attempt fails must be tried again and again, not abandoned after \
         the first error: {} attempts",
        worker.attempts
    );
    assert!(
        count_for("ttl_expiry") - before >= 3,
        "and each retry is counted, so a permanently failing worker shows up as a rising count \
         rather than as silence"
    );
}

#[tokio::test(start_paused = true)]
async fn a_quiet_stretch_resets_the_backoff() {
    // The backoff doubled to its maximum and stayed there for the life of the
    // process. A task that failed a few times an hour ago then waited the full
    // two minutes for an unrelated transient error -- and the error that
    // recovers on the first retry is exactly the one most likely to be waited
    // out for two minutes for nothing.
    //
    // On tokio's clock, paused, so the waits are the assertion rather than a
    // race: `sleep` advances time when the runtime is idle.
    let shutdown = kimmy_task::Shutdown::new();
    let mut retry = kimmy_task::Retry::new(
        "retention_collector",
        Duration::from_secs(1),
        Duration::from_secs(60),
    );

    let mut waits = Vec::new();
    for _ in 0..3 {
        let at = tokio::time::Instant::now();
        assert!(retry.after("a transient failure", &shutdown).await);
        waits.push(tokio::time::Instant::now() - at);
    }
    assert_eq!(
        waits,
        vec![Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)],
        "premise: the backoff doubles while the failures keep coming"
    );

    // Quiet for longer than four times the wait it had reached. Three failures
    // leave the *next* wait at 8s, so the stretch to beat is 32s -- and 17s,
    // which is four times the last wait actually taken, is not enough. The
    // first version of this test used that and failed, which is the assertion
    // doing its job: the rule is about the wait ahead, not the one behind.
    tokio::time::sleep(Duration::from_secs(4 * 8 + 1)).await;

    let at = tokio::time::Instant::now();
    assert!(retry.after("a transient failure much later", &shutdown).await);
    assert_eq!(
        tokio::time::Instant::now() - at,
        Duration::from_secs(1),
        "after a quiet stretch the next failure waits the first backoff again, not the eight \
         seconds the schedule had climbed to"
    );
}

fn count_for(task: &str) -> u64 {
    kimmy_task::retries().into_iter().find(|(name, _)| *name == task).map_or(0, |(_, n)| n)
}
