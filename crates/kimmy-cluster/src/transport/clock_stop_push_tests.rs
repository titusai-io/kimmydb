//! U1 at the push receiver (package 3.0): a refusal that reaches it as an *error* is answered with the normal
//! `Pushed` frame carrying the stop, never a fault. Its own file because it needs the push-side test hook that
//! lives in the production file (`test_hooks::ClockStoppingAPush`): proved by mutant (MC3), not failing-first.

use kimmy_core::{Hlc, VersionVector};
use kimmy_storage::Engine;

use super::*;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

const SECRET: &str = "a-clock-stop-push-secret";
const BINDING: &[u8] = b"a-clock-stop-push-binding";

/// A push that meets U1's refusal as an *error* (a hook makes the receiver's apply return it, which the batch loop
/// never does) is answered with the normal `Pushed` frame carrying the stop, never a fault: the pusher must not
/// read a member that is only waiting for a clock to be due as failed.
#[tokio::test]
async fn a_push_whose_apply_returns_the_clock_refusal_is_answered_pushed_and_not_fault() {
    let (b, _db) = engine();
    let (c, _dc) = engine();
    let _stopping = crate::transport::test_hooks::ClockStoppingAPush::at(b.node_id());
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let serving = serve_peer(&b, far, SECRET, BINDING, None, ServeBudgets::serving(), None);
    let pushing = async {
        open_handshake(&c, &mut ours, SECRET, BINDING).await.unwrap();
        let push = Message::Push {
            entries: Vec::new(),
            scanned_to: Hlc::ZERO,
            exhausted: true,
            versions: VersionVector::new(),
        };
        write_frame(&mut ours, &push).await.unwrap();
        let reply = read_frame(&mut ours).await.unwrap();
        drop(ours);
        reply
    };
    let (served, reply) = tokio::join!(serving, pushing);
    match reply {
        Message::Pushed { clock_refused, clock_due_in_ms, stopped_at, .. } => {
            assert_eq!(
                clock_refused, 1,
                "the reply is a Pushed frame, not a Fault: it says the stop"
            );
            assert!(stopped_at.is_some(), "and names the entry it stopped at");
            assert_eq!(
                clock_due_in_ms, 1,
                "and how long, on the receiver's clock, until that entry is due (the hook's stop is due in 1 ms)"
            );
        }
        other => panic!("the reply is a Pushed frame, not a Fault: {other:?}"),
    }
    assert!(
        matches!(served, Err(ProtocolError::Closed)) || served.is_ok(),
        "and the serving side did not end in a failure: {served:?}"
    );
}
