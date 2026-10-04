//! The stall point of `KIMMY_TEST_KILL_TASK` and the supervised thread (ADR-213).
//!
//! Each scenario re-runs this binary as a child with its own environment, because
//! the switch is read once per process and a thread's death is an exit status.

use std::process::Command;
use std::time::Duration;

fn probe_with(scenario: &str, test_name: &str, env: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(std::env::current_exe().expect("this test binary"));
    command.args(["--exact", test_name, "--nocapture"]).env("KIMMY_TASK_PROBE", scenario);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("re-running this test binary")
}

fn scenario_is(want: &str) -> bool {
    std::env::var("KIMMY_TASK_PROBE").is_ok_and(|s| s == want)
}

/// A panic in a supervised thread is a death: exit 70, naming the thread.
#[test]
fn a_panic_in_a_supervised_thread_exits_70_and_names_it() {
    if scenario_is("thread_panics") {
        let _ =
            kimmy_task::supervise_thread("yield_evaluator", kimmy_task::Shutdown::new(), || {
                panic!("the thread panics on purpose");
            });
        std::thread::sleep(Duration::from_secs(10));
        return;
    }
    let out =
        probe_with("thread_panics", "a_panic_in_a_supervised_thread_exits_70_and_names_it", &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(70), "{stderr}");
    assert!(
        stderr.contains(
            r#"the supervised task "yield_evaluator" panicked (the thread panics on purpose)"#
        ),
        "{stderr}"
    );
}

/// A return is not a death, and a panic that lands after shutdown has begun is part
/// of the stop: neither ends the process.
#[test]
fn a_thread_that_returns_or_panics_during_shutdown_does_not_exit() {
    if scenario_is("thread_quiet") {
        let ended =
            kimmy_task::supervise_thread("yield_evaluator", kimmy_task::Shutdown::new(), || {})
                .unwrap();
        ended.join().unwrap();
        let shutdown = kimmy_task::Shutdown::new();
        shutdown.begin();
        let panicked = kimmy_task::supervise_thread("yield_evaluator", shutdown, || {
            panic!("a panic during the stop");
        })
        .unwrap();
        panicked.join().unwrap();
        println!("SURVIVED");
        return;
    }
    let out = probe_with(
        "thread_quiet",
        "a_thread_that_returns_or_panics_during_shutdown_does_not_exit",
        &[],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("SURVIVED"), "{stdout}");
}

/// A thread is named with hyphens, and is recorded as started.
#[test]
fn a_supervised_thread_is_named_and_recorded() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let handle = kimmy_task::supervise_thread("yield_evaluator", kimmy_task::Shutdown::new(), {
        let seen = std::sync::Arc::clone(&seen);
        move || *seen.lock().unwrap() = std::thread::current().name().unwrap_or("").to_string()
    })
    .unwrap();
    handle.join().unwrap();
    assert_eq!(*seen.lock().unwrap(), "yield-evaluator");
    assert!(kimmy_task::started().contains(&"yield_evaluator"));
}

/// An unbounded stall blocks the task at its stall point until the stop's first
/// signal, and not before: the task is seen to have arrived (the positive control)
/// and still not through.
#[test]
fn an_unbounded_stall_holds_the_task_until_the_stop_begins() {
    if scenario_is("stall_unbounded") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            kimmy_task::arm_test_kills();
            let shutdown = kimmy_task::Shutdown::new();
            let (arrived, through) = (
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            );
            let task = tokio::spawn({
                let (shutdown, arrived, through) =
                    (shutdown.clone(), arrived.clone(), through.clone());
                async move {
                    arrived.store(true, std::sync::atomic::Ordering::SeqCst);
                    kimmy_task::stall_point("ttl_expiry", &shutdown).await;
                    through.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            });
            while !arrived.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            // Plenty of turns for a task that was going to get through.
            for _ in 0..200 {
                tokio::task::yield_now().await;
            }
            assert!(!through.load(std::sync::atomic::Ordering::SeqCst), "it got through the stall");
            shutdown.begin();
            task.await.unwrap();
            assert!(through.load(std::sync::atomic::Ordering::SeqCst));
            println!("HELD-THEN-RELEASED");
        });
        return;
    }
    let out = probe_with(
        "stall_unbounded",
        "an_unbounded_stall_holds_the_task_until_the_stop_begins",
        &[("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall")],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("HELD-THEN-RELEASED"), "{stdout}");
}

/// A bounded stall ends by itself, once: the same task coming round again is not
/// stalled again. Paused time, so nothing here waits.
#[test]
fn a_bounded_stall_ends_by_itself_and_does_not_begin_again() {
    if scenario_is("stall_bounded") {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            kimmy_task::arm_test_kills();
            let shutdown = kimmy_task::Shutdown::new();
            let began = tokio::time::Instant::now();
            // On the paused clock a stall that never ends is a timeout, not a hang.
            let first = tokio::time::timeout(
                Duration::from_secs(3600),
                kimmy_task::stall_point("webhook_dispatcher", &shutdown),
            )
            .await;
            if first.is_err() {
                println!("HUNG: the bounded stall did not end by itself");
                return;
            }
            let held = began.elapsed();
            kimmy_task::stall_point("webhook_dispatcher", &shutdown).await;
            let again = began.elapsed();
            // A task the switch does not name is not held at all.
            kimmy_task::stall_point("ttl_expiry", &shutdown).await;
            println!("HELD={} AGAIN={} OTHER={}", held.as_secs(), (again - held).as_secs(), 0);
        });
        return;
    }
    let out = probe_with(
        "stall_bounded",
        "a_bounded_stall_ends_by_itself_and_does_not_begin_again",
        &[("KIMMY_TEST_KILL_TASK", "webhook_dispatcher:stall:30")],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(
        stdout.contains("HELD=30 AGAIN=0"),
        "the stall lasted its 30 s of the clock, once: {stdout}"
    );
}

/// Before the node serves nothing is armed: a stall point lets the task through.
#[test]
fn an_unarmed_switch_stalls_nothing() {
    if scenario_is("unarmed") {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            kimmy_task::stall_point("ttl_expiry", &kimmy_task::Shutdown::new()).await;
            println!("THROUGH");
        });
        return;
    }
    let out = probe_with(
        "unarmed",
        "an_unarmed_switch_stalls_nothing",
        &[("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall")],
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("THROUGH"));
}
