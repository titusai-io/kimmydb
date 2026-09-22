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
    Command::new(std::env::current_exe().expect("this test binary"))
        .args(["--exact", test_name, "--nocapture"])
        .env("KIMMY_TASK_PROBE", scenario)
        .output()
        .expect("re-running this test binary")
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
    // the node. Two of the membership tasks do exactly that.
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

fn count_for(task: &str) -> u64 {
    kimmy_task::retries().into_iter().find(|(name, _)| *name == task).map_or(0, |(_, n)| n)
}
