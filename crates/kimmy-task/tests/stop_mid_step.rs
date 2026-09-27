//! A supervised task inside a storage step when the stop comes, and the
//! runtime shut down behind it, as a node's stop does (0.40.1's panic).
//!
//! Its own binary: it installs a panic hook, which is the process's.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static PANICS: AtomicUsize = AtomicUsize::new(0);

/// The work is the shape of a replication round: a `biased` select of the
/// round, which applies a batch in `block_in_place`, and its deadline. The
/// stop comes mid-apply. Once the supervisor has returned, the runtime is shut
/// down, and the work must not be left to poll its deadline behind it: the
/// supervisor returns only once the work is gone.
#[test]
fn a_supervisor_returns_only_once_its_work_has_stopped() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        PANICS.fetch_add(1, Ordering::SeqCst);
        default(info);
    }));
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let shutdown = kimmy_task::Shutdown::new();
    let (entered, stepping) = std::sync::mpsc::channel();
    let handle = {
        let _entered = runtime.enter();
        kimmy_task::supervise("stepping", shutdown.clone(), async move {
            tokio::select! {
                biased;
                () = async {
                    tokio::task::block_in_place(|| {
                        let _ = entered.send(());
                        std::thread::sleep(Duration::from_millis(300));
                    });
                    std::future::pending::<()>().await
                } => {}
                () = tokio::time::sleep(Duration::from_secs(60)) => {}
            }
        })
    };
    stepping.recv().unwrap();
    // As `node::run` does: begin the stop, wait for the supervisor within the
    // stop's time, and only then shut the runtime down.
    shutdown.begin();
    let supervisor =
        runtime.block_on(async { tokio::time::timeout(Duration::from_secs(5), handle).await });
    assert!(supervisor.is_ok(), "the supervisor never returned");
    runtime.shutdown_timeout(Duration::from_secs(2));
    assert_eq!(
        PANICS.load(Ordering::SeqCst),
        0,
        "a task polled a timer after the runtime shut down"
    );
}
