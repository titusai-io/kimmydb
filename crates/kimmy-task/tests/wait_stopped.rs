//! A caller can wait for the supervisors started under a [`Shutdown`] to have
//! ended, as the stop path does before it shuts the runtime down, and a start
//! that failed after it spawned its tasks must too: a task inside a storage step
//! runs on after an abort until its next yield, and a timer it polls once the
//! runtime is going away panics.

use std::time::Duration;

use kimmy_task::Shutdown;
use tokio::time::Instant;

/// Not stopped while a supervisor runs, stopped once shutdown has begun and it
/// has ended: the wait answers what is true, in time or out of it.
#[tokio::test]
async fn wait_stopped_is_false_while_a_supervisor_runs_and_true_once_it_has_ended() {
    let shutdown = Shutdown::new();
    let running = kimmy_task::supervise("wait_stopped_a", shutdown.clone(), async {
        std::future::pending::<()>().await
    });
    assert!(
        !shutdown.wait_stopped(Instant::now() + Duration::from_millis(100)).await,
        "a supervisor that is still running was counted as stopped"
    );
    shutdown.begin();
    assert!(
        shutdown.wait_stopped(Instant::now() + Duration::from_secs(10)).await,
        "the supervisor did not end after shutdown began"
    );
    running.await.unwrap();
}

/// A supervisor counted at its spawn, before it has ever been polled, and then
/// dropped without running, still ends the count: the guard travels with the
/// future, not with its first poll.
#[tokio::test(flavor = "current_thread")]
async fn a_supervisor_dropped_before_it_ran_does_not_hold_the_wait() {
    let shutdown = Shutdown::new();
    let never_polled = kimmy_task::supervise("wait_stopped_b", shutdown.clone(), async {
        std::future::pending::<()>().await
    });
    never_polled.abort();
    assert!(
        shutdown.wait_stopped(Instant::now() + Duration::from_secs(10)).await,
        "a supervisor that never ran held the wait"
    );
}

/// The count is per shutdown: supervisors under another one are not waited for.
#[tokio::test]
async fn wait_stopped_counts_only_its_own_supervisors() {
    let mine = Shutdown::new();
    let other = Shutdown::new();
    let elsewhere = kimmy_task::supervise("wait_stopped_c", other.clone(), async {
        std::future::pending::<()>().await
    });
    assert!(
        mine.wait_stopped(Instant::now() + Duration::from_millis(100)).await,
        "another shutdown's supervisor was waited for"
    );
    other.begin();
    elsewhere.await.unwrap();
}
