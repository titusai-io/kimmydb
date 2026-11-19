//! Step 3, the frame check: what a peer's frame claims of its clock, as `Engine` reads it: the two vectors of
//! one frame are counted once, and a replay position past the bound is not believed.

use kimmy_core::{Hlc, Stamp};

use crate::{Engine, physical_now_ms};

const SKEW_MS: u64 = 300_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

#[test]
fn the_two_vectors_of_a_frame_are_counted_once() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let origin = a.origin();
    let mut far = a.version_vector().unwrap();
    far.insert(origin, Hlc::new(physical_now_ms() + 3_600_000, 0));

    let (first, second) = b.clamp_peer_vectors(&far, &far);

    let limit = b.clock_bound_now_ms() + SKEW_MS;
    assert!(first.get(origin).wall_ms <= limit && second.get(origin).wall_ms <= limit);
    assert_eq!(b.clock_refusals(), vec![(origin.node.to_string(), 1)], "one frame, one count");
}

#[test]
fn a_replay_position_is_believed_within_the_bound_and_not_past_it() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let origin = a.origin();
    let within = Stamp::new(Hlc::new(physical_now_ms() + 60_000, 0), origin.node, origin.epoch);
    let past = Stamp::new(Hlc::new(physical_now_ms() + 3_600_000, 0), origin.node, origin.epoch);

    assert_eq!(b.believed_replay_from(None), None);
    assert_eq!(b.believed_replay_from(Some(within)), Some(within));
    assert!(b.clock_refusals().is_empty(), "a position within the bound is not a refusal");
    let every_epoch = Stamp::new(within.hlc, origin.node, kimmy_core::Epoch::MAX);
    assert_eq!(
        b.believed_replay_from(Some(every_epoch)),
        Some(every_epoch),
        "the position a member asks a replay at is in every epoch, and its epoch is no mint"
    );
    assert_eq!(b.believed_replay_from(Some(past)), None);
    assert_eq!(b.clock_refusals(), vec![(origin.node.to_string(), 1)]);
}
