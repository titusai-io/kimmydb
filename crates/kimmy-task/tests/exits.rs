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
    // The control for the announcement. Shutdown aborts supervised tasks, so
    // without the announcement being read each abort would be a death.
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

fn count_for(task: &str) -> u64 {
    kimmy_task::retries().into_iter().find(|(name, _)| *name == task).map_or(0, |(_, n)| n)
}
