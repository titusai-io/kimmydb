//! Step 3, package 3.0 (U1): how far ahead of this member's physical clock a replicated entry or
//! state of a `field` collection may be.
//!
//! Under `field` merge the horizon of a document is `H = max(vv) - R`, computed from the merged
//! state, so a state that holds one far-future write moves `H` far forward the moment it is
//! joined and every delete record below it is pruned, on every member that joins it. A pruned
//! delete cannot be un-pruned, so the join is not made: the entry or state is **refused until
//! due**, by name, not applied and not witnessed, and it applies once the receiver's physical
//! clock has caught up to within the bound.
//!
//! This module is the one function that decides, and the types that name the answer. It reads
//! no clock and no engine: the caller gives it the receiver's physical now (the engine's own,
//! which a test can move, see [`crate::Engine::set_clock_offset_for_tests`]) and the bound.
//! Nothing here applies anything; the call sites do (the apply of a field entry, the state-only
//! apply of a snapshot page document), and they place the refusal where every field state passes.

use std::collections::BTreeMap;
use std::fmt;

use kimmy_core::{Origin, Stamp};

/// The default of `storage.max_clock_skew_secs`, in milliseconds: five minutes. The same value
/// is the skew margin of the tombstone retention check (3.6).
pub const DEFAULT_MAX_CLOCK_SKEW_MS: u64 = 300_000;

/// How many origins a refusal counter names before the rest are summed under `other`: the
/// label comes from a peer's stamp, so its cardinality is the peer's to choose.
pub const MAX_REFUSAL_LABELS: usize = 64;

/// The label the origins past [`MAX_REFUSAL_LABELS`] are summed under.
pub const OTHER_ORIGINS: &str = "other";

/// Why an entry or state was refused, in the order the checks run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockRefusal {
    /// The newest write the merged state has seen, of any origin, is past the bound.
    Vv,
    /// The entry's own stamp is past the bound.
    Stamp,
    /// The entry's origin was minted (its epoch starts) past the bound.
    Epoch,
}

impl ClockRefusal {
    /// The name a log line, a test and an operator read: `vv`, `stamp` or `epoch`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Vv => "vv",
            Self::Stamp => "stamp",
            Self::Epoch => "epoch",
        }
    }
}

impl fmt::Display for ClockRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An entry or state that was refused, and what is needed to say why and until when.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClockStop {
    /// The origin of the refused stamp.
    pub origin: Origin,
    /// The refused entry's stamp: the window ends just before it.
    pub stamp: Stamp,
    /// Which of the three checks refused it.
    pub reason: ClockRefusal,
    /// How long until it is due, at the physical clock that refused it, in milliseconds.
    pub due_in_ms: u64,
}

/// The bound: how many milliseconds ahead of the receiver's physical clock a stamp may be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockBound {
    pub skew_ms: u64,
}

impl Default for ClockBound {
    fn default() -> Self {
        Self { skew_ms: DEFAULT_MAX_CLOCK_SKEW_MS }
    }
}

impl ClockBound {
    /// `None` when `stamp` (and the state behind it) is within the bound at `now_ms`; otherwise
    /// the refusal, naming the first check that failed.
    ///
    /// `vv_max_wall_ms` is the newest wall time any origin of the entry's merged state has
    /// written, or `None` for an entry that carries no state (a carried delete). The bound is
    /// inclusive: a stamp exactly `skew_ms` ahead applies, one millisecond more does not.
    pub fn check(
        &self,
        now_ms: u64,
        stamp: &Stamp,
        vv_max_wall_ms: Option<u64>,
    ) -> Option<ClockStop> {
        let limit = now_ms.saturating_add(self.skew_ms);
        let offenders = [
            (ClockRefusal::Vv, vv_max_wall_ms),
            (ClockRefusal::Stamp, Some(stamp.hlc.wall_ms)),
            (ClockRefusal::Epoch, Some(stamp.epoch.start_hlc().wall_ms)),
        ];
        offenders.into_iter().find_map(|(reason, wall)| {
            let wall = wall.filter(|wall| *wall > limit)?;
            Some(ClockStop {
                origin: stamp.origin(),
                stamp: *stamp,
                reason,
                due_in_ms: wall - limit,
            })
        })
    }
}

/// The refusals an engine has made since it started, by the origin that sent them. At most
/// [`MAX_REFUSAL_LABELS`] origins are named; the rest are summed under [`OTHER_ORIGINS`].
#[derive(Debug, Default)]
pub(crate) struct RefusalCounts {
    by_origin: BTreeMap<String, u64>,
    other: u64,
}

impl RefusalCounts {
    pub(crate) fn add(&mut self, origin: &Origin, n: u64) {
        let label = origin.node.to_string();
        if let Some(count) = self.by_origin.get_mut(&label) {
            *count += n;
        } else if self.by_origin.len() < MAX_REFUSAL_LABELS {
            self.by_origin.insert(label, n);
        } else {
            self.other += n;
        }
    }

    /// The counts by origin label, in label order, with `other` last when it is not zero.
    pub(crate) fn snapshot(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> =
            self.by_origin.iter().map(|(label, count)| (label.clone(), *count)).collect();
        if self.other > 0 {
            out.push((OTHER_ORIGINS.to_string(), self.other));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kimmy_core::{Epoch, Hlc, NodeId};

    const NOW: u64 = 1_000_000;

    fn stamp(wall_ms: u64) -> Stamp {
        Stamp::new(Hlc::new(wall_ms, 0), NodeId::generate(), Epoch::ZERO)
    }

    #[test]
    fn the_bound_is_inclusive_and_one_ms_past_it_is_refused() {
        let bound = ClockBound { skew_ms: 300_000 };
        let edge = NOW + 300_000;
        assert_eq!(bound.check(NOW, &stamp(edge), None), None, "exactly at the bound applies");
        let past = bound.check(NOW, &stamp(edge + 1), None).expect("one ms past is refused");
        assert_eq!((past.reason, past.due_in_ms), (ClockRefusal::Stamp, 1));
    }

    #[test]
    fn the_state_is_checked_before_the_stamp_and_a_missing_state_is_skipped() {
        let bound = ClockBound { skew_ms: 10 };
        let far = NOW + 1_000;
        let both = bound.check(NOW, &stamp(far), Some(far)).unwrap();
        assert_eq!(both.reason, ClockRefusal::Vv, "the first check that fails names the reason");
        let state_only = bound.check(NOW, &stamp(NOW), Some(far)).unwrap();
        assert_eq!(state_only.reason, ClockRefusal::Vv);
        let stamp_only = bound.check(NOW, &stamp(far), None).unwrap();
        assert_eq!(stamp_only.reason, ClockRefusal::Stamp, "a carried delete has no state");
        assert_eq!(bound.check(NOW, &stamp(NOW), Some(NOW)), None);
    }

    #[test]
    fn an_epoch_that_starts_past_the_bound_is_refused_by_name() {
        let bound = ClockBound { skew_ms: 10 };
        let epoch = Epoch::from_u64(0);
        assert_eq!(epoch.start_hlc(), Hlc::ZERO, "premise: an integer epoch starts at zero");
        let minted = Epoch::new(Hlc::new(NOW + 1_000, 0), [7; 8]);
        let far = Stamp::new(Hlc::new(NOW, 0), NodeId::generate(), minted);
        let stop = bound.check(NOW, &far, None).expect("an epoch minted in the future is refused");
        assert_eq!((stop.reason, stop.due_in_ms), (ClockRefusal::Epoch, 990));
    }

    #[test]
    fn the_arithmetic_saturates() {
        let bound = ClockBound { skew_ms: u64::MAX };
        assert_eq!(bound.check(u64::MAX, &stamp(u64::MAX), Some(u64::MAX)), None);
    }

    #[test]
    fn the_counts_name_64_origins_and_sum_the_rest() {
        let mut counts = RefusalCounts::default();
        let origins: Vec<Origin> = (0..MAX_REFUSAL_LABELS + 3)
            .map(|_| Origin { node: NodeId::generate(), epoch: Epoch::ZERO })
            .collect();
        for origin in &origins {
            counts.add(origin, 2);
        }
        counts.add(&origins[0], 5);
        let snapshot = counts.snapshot();
        assert_eq!(snapshot.len(), MAX_REFUSAL_LABELS + 1, "64 named and `other`");
        assert_eq!(snapshot.last().unwrap(), &(OTHER_ORIGINS.to_string(), 6));
        let first = snapshot.iter().find(|(label, _)| *label == origins[0].node.to_string());
        assert_eq!(first.map(|(_, n)| *n), Some(7), "a named origin keeps counting");
    }
}
