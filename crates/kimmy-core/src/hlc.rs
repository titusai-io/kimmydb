//! Hybrid Logical Clocks.
//!
//! An HLC gives every write a timestamp that is *close* to wall-clock time but,
//! unlike wall-clock time, is strictly monotonic on a node and causally
//! consistent across nodes. That combination is what lets KimmyDB resolve
//! conflicts with last-writer-wins without a leader: any two writes anywhere in
//! the cluster have a well-defined order.
//!
//! The clock itself is pure — callers pass physical time in. That keeps this
//! crate I/O-free and, more usefully, makes clock-skew scenarios trivial to
//! test deterministically.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ids::NodeId;

/// A hybrid logical timestamp.
///
/// Ordering is lexicographic on `(wall_ms, counter)`. That ordering is
/// preserved by [`Hlc::to_bytes`], so encoded timestamps can be used directly
/// as sort keys in the storage layer.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hlc {
    /// Physical component: milliseconds since the Unix epoch.
    pub wall_ms: u64,
    /// Logical component: disambiguates writes within the same millisecond.
    pub counter: u16,
}

/// Width of the [`Hlc::to_bytes`] encoding.
pub const HLC_ENCODED_LEN: usize = 10;

impl Hlc {
    /// The earliest representable timestamp. Sorts before every other value.
    pub const ZERO: Hlc = Hlc { wall_ms: 0, counter: 0 };
    /// The latest representable timestamp. Sorts after every other value.
    pub const MAX: Hlc = Hlc { wall_ms: u64::MAX, counter: u16::MAX };

    pub const fn new(wall_ms: u64, counter: u16) -> Self {
        Self { wall_ms, counter }
    }

    /// Encode to 10 big-endian bytes whose `memcmp` order matches [`Ord`].
    pub fn to_bytes(self) -> [u8; HLC_ENCODED_LEN] {
        let mut out = [0u8; HLC_ENCODED_LEN];
        out[..8].copy_from_slice(&self.wall_ms.to_be_bytes());
        out[8..].copy_from_slice(&self.counter.to_be_bytes());
        out
    }

    /// Decode from the [`Hlc::to_bytes`] representation.
    pub fn from_bytes(bytes: [u8; HLC_ENCODED_LEN]) -> Self {
        let mut wall = [0u8; 8];
        wall.copy_from_slice(&bytes[..8]);
        let mut ctr = [0u8; 2];
        ctr.copy_from_slice(&bytes[8..]);
        Self { wall_ms: u64::from_be_bytes(wall), counter: u16::from_be_bytes(ctr) }
    }

    /// The next representable timestamp. Used to build exclusive range bounds
    /// when resuming a change stream *after* a given token.
    pub fn successor(self) -> Self {
        match self.counter.checked_add(1) {
            Some(counter) => Self { wall_ms: self.wall_ms, counter },
            None => Self { wall_ms: self.wall_ms.saturating_add(1), counter: 0 },
        }
    }
}

impl fmt::Debug for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hlc({}.{})", self.wall_ms, self.counter)
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.wall_ms, self.counter)
    }
}

/// A write's full ordering key: the timestamp plus the node that produced it.
///
/// Two nodes can legitimately produce the same [`Hlc`]; the node id breaks that
/// tie so that last-writer-wins is deterministic — every replica picks the same
/// winner regardless of the order updates arrive in.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Stamp {
    pub hlc: Hlc,
    pub node: NodeId,
}

impl Stamp {
    pub const fn new(hlc: Hlc, node: NodeId) -> Self {
        Self { hlc, node }
    }

    /// True if `self` should overwrite `other` under last-writer-wins.
    ///
    /// Note the strict comparison: an identical stamp does *not* win, which is
    /// what makes applying the same oplog entry twice a no-op.
    pub fn wins_over(&self, other: &Stamp) -> bool {
        self > other
    }
}

/// A monotonic hybrid logical clock.
///
/// Physical time is supplied by the caller rather than read internally, so the
/// clock is deterministic and this crate stays free of I/O.
#[derive(Clone, Debug, Default)]
pub struct HlcClock {
    last: Hlc,
}

impl HlcClock {
    pub fn new() -> Self {
        Self { last: Hlc::ZERO }
    }

    /// Start from a previously persisted timestamp, so restarts never reuse or
    /// move backwards through the timestamp space.
    pub fn resuming_from(last: Hlc) -> Self {
        Self { last }
    }

    /// The most recent timestamp this clock issued or observed.
    pub fn last(&self) -> Hlc {
        self.last
    }

    /// Issue a timestamp for a local write.
    ///
    /// If the physical clock has advanced, the logical counter resets; if it has
    /// stalled or jumped backwards (NTP correction, VM migration), the counter
    /// increments instead. Either way the result is strictly greater than every
    /// timestamp previously issued or observed.
    pub fn tick(&mut self, physical_ms: u64) -> Hlc {
        let next = if physical_ms > self.last.wall_ms {
            Hlc { wall_ms: physical_ms, counter: 0 }
        } else {
            // Physical time did not advance past our last stamp. Bump the
            // logical counter, rolling into the next millisecond if it is
            // exhausted (2^16 writes in one millisecond on one node).
            self.last.successor()
        };
        self.last = next;
        next
    }

    /// Fold a timestamp received from a peer into this clock, then issue a
    /// timestamp that is strictly greater than both it and our previous state.
    ///
    /// Calling this on every inbound replication message is what makes causality
    /// transitive across the cluster.
    pub fn observe(&mut self, physical_ms: u64, remote: Hlc) -> Hlc {
        let high_water = self.last.max(remote);
        let next = if physical_ms > high_water.wall_ms {
            Hlc { wall_ms: physical_ms, counter: 0 }
        } else {
            high_water.successor()
        };
        self.last = next;
        next
    }

    /// Advance the clock past a remote timestamp without issuing a new one.
    ///
    /// Used when applying replicated writes: we must not fall behind a peer's
    /// clock, but the write already carries its own originating timestamp and
    /// must keep it.
    pub fn witness(&mut self, remote: Hlc) {
        if remote > self.last {
            self.last = remote;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    #[test]
    fn tick_advances_with_physical_time() {
        let mut clock = HlcClock::new();
        let a = clock.tick(1_000);
        let b = clock.tick(1_001);
        assert_eq!(a, Hlc::new(1_000, 0));
        assert_eq!(b, Hlc::new(1_001, 0));
    }

    #[test]
    fn tick_uses_counter_within_one_millisecond() {
        let mut clock = HlcClock::new();
        assert_eq!(clock.tick(1_000), Hlc::new(1_000, 0));
        assert_eq!(clock.tick(1_000), Hlc::new(1_000, 1));
        assert_eq!(clock.tick(1_000), Hlc::new(1_000, 2));
    }

    #[test]
    fn tick_is_monotonic_when_physical_clock_jumps_backwards() {
        let mut clock = HlcClock::new();
        let a = clock.tick(5_000);
        // NTP yanks the clock back a full second.
        let b = clock.tick(4_000);
        let c = clock.tick(4_001);
        assert!(b > a, "{b:?} must exceed {a:?} despite the backwards jump");
        assert!(c > b);
        assert_eq!(b, Hlc::new(5_000, 1));
    }

    #[test]
    fn observe_advances_past_a_future_peer() {
        let mut clock = HlcClock::new();
        clock.tick(1_000);
        // Peer's clock is 10s ahead of ours.
        let merged = clock.observe(1_001, Hlc::new(11_000, 7));
        assert!(merged > Hlc::new(11_000, 7));
        assert_eq!(merged, Hlc::new(11_000, 8));
    }

    #[test]
    fn witness_never_moves_the_clock_backwards() {
        let mut clock = HlcClock::new();
        clock.tick(9_000);
        clock.witness(Hlc::new(1, 0));
        assert_eq!(clock.last(), Hlc::new(9_000, 0));
    }

    #[test]
    fn counter_exhaustion_rolls_into_the_next_millisecond() {
        let mut clock = HlcClock::resuming_from(Hlc::new(42, u16::MAX));
        let next = clock.tick(42);
        assert_eq!(next, Hlc::new(43, 0));
        assert!(next > Hlc::new(42, u16::MAX));
    }

    #[test]
    fn byte_encoding_round_trips() {
        let hlc = Hlc::new(0x0123_4567_89ab_cdef, 0xbeef);
        assert_eq!(Hlc::from_bytes(hlc.to_bytes()), hlc);
    }

    #[test]
    fn stamp_breaks_ties_by_node_id() {
        let hlc = Hlc::new(100, 0);
        let lo = Stamp::new(hlc, node(1));
        let hi = Stamp::new(hlc, node(2));
        assert!(hi.wins_over(&lo));
        assert!(!lo.wins_over(&hi));
        // Reapplying an identical write must be a no-op, not a flip-flop.
        assert!(!hi.wins_over(&hi));
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        prop_compose! {
            fn any_hlc()(wall_ms in any::<u64>(), counter in any::<u16>()) -> Hlc {
                Hlc { wall_ms, counter }
            }
        }

        proptest! {
            /// The encoding must be order-preserving: the storage layer sorts
            /// oplog entries by raw bytes and relies on that matching `Ord`.
            #[test]
            fn encoding_preserves_order(a in any_hlc(), b in any_hlc()) {
                prop_assert_eq!(a.cmp(&b), a.to_bytes().cmp(&b.to_bytes()));
            }

            #[test]
            fn encoding_round_trips(a in any_hlc()) {
                prop_assert_eq!(Hlc::from_bytes(a.to_bytes()), a);
            }

            /// No sequence of physical timestamps, however adversarial, may
            /// produce a non-increasing sequence of HLCs.
            #[test]
            fn tick_is_strictly_monotonic(times in prop::collection::vec(any::<u32>(), 1..200)) {
                let mut clock = HlcClock::new();
                let mut prev = Hlc::ZERO;
                for t in times {
                    let next = clock.tick(u64::from(t));
                    prop_assert!(next > prev, "{next:?} did not exceed {prev:?}");
                    prev = next;
                }
            }

            /// Interleaving local ticks with observed peer timestamps must also
            /// stay strictly monotonic, and must always dominate what we saw.
            #[test]
            fn observe_dominates_local_and_remote(
                ops in prop::collection::vec((any::<u32>(), any::<bool>(), any_hlc()), 1..200)
            ) {
                let mut clock = HlcClock::new();
                let mut prev = Hlc::ZERO;
                for (t, is_remote, remote) in ops {
                    let next = if is_remote {
                        let r = clock.observe(u64::from(t), remote);
                        prop_assert!(r > remote, "{r:?} did not exceed observed {remote:?}");
                        r
                    } else {
                        clock.tick(u64::from(t))
                    };
                    prop_assert!(next > prev, "{next:?} did not exceed {prev:?}");
                    prev = next;
                }
            }

            /// `successor` must be the immediate next value: nothing sorts
            /// between a timestamp and its successor. Resume-after semantics
            /// depend on this — a gap here would silently skip an event.
            #[test]
            fn successor_is_immediate(a in any_hlc()) {
                prop_assume!(a != Hlc::MAX);
                let s = a.successor();
                prop_assert!(s > a);
                if a.counter < u16::MAX {
                    prop_assert_eq!(s, Hlc::new(a.wall_ms, a.counter + 1));
                }
            }
        }
    }
}
