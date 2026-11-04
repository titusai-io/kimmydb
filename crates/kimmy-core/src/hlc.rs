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
use crate::origin::{Epoch, Origin};

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

    /// The previous representable timestamp; [`Hlc::ZERO`] stays where it
    /// is. The inverse of [`Hlc::successor`], used to name "everything
    /// strictly before this stamp" as an inclusive position: a sync window
    /// that ends *before* an entry it did not carry is witnessed to here,
    /// so the next request, which is inclusive at the position it names,
    /// re-serves that entry rather than skipping it.
    pub fn predecessor(self) -> Self {
        match self.counter.checked_sub(1) {
            Some(counter) => Self { wall_ms: self.wall_ms, counter },
            None if self.wall_ms == 0 => Self::ZERO,
            None => Self { wall_ms: self.wall_ms - 1, counter: u16::MAX },
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

/// A write's full ordering key: the timestamp, the node that produced it and that node's epoch.
///
/// Two nodes can legitimately produce the same [`Hlc`]; the node id breaks that
/// tie so that last-writer-wins is deterministic — every replica picks the same
/// winner regardless of the order updates arrive in. The epoch is the last
/// tie-break and, with the node, the stamp's [`Origin`]: a restored node takes a
/// fresh epoch, so its new writes are never taken for writes it lost.
///
/// The derived order is `(hlc, node, epoch)`, which is the byte order of
/// [`Stamp::to_key_bytes`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Stamp {
    pub hlc: Hlc,
    pub node: NodeId,
    pub epoch: Epoch,
}

/// The first byte of the [`Stamp::encode`] token: the format of what follows.
const STAMP_TOKEN_FORMAT: u8 = 0x01;

impl Stamp {
    /// Width of the key form, [`Stamp::to_key_bytes`]: `hlc ‖ node ‖ epoch`.
    pub const KEY_LEN: usize = HLC_ENCODED_LEN + Origin::LEN;

    /// Width of the [`Stamp::encode`] token before base64: the format byte, then the key form.
    pub const TOKEN_LEN: usize = 1 + Self::KEY_LEN;

    pub const fn new(hlc: Hlc, node: NodeId, epoch: Epoch) -> Self {
        Self { hlc, node, epoch }
    }

    /// The writer incarnation that produced this stamp.
    pub const fn origin(&self) -> Origin {
        Origin { node: self.node, epoch: self.epoch }
    }

    /// True if `self` should overwrite `other` under last-writer-wins.
    ///
    /// Note the strict comparison: an identical stamp does *not* win, which is
    /// what makes applying the same oplog entry twice a no-op.
    pub fn wins_over(&self, other: &Stamp) -> bool {
        self > other
    }

    /// The key form: 44 bytes, the HLC's order-preserving encoding, the node id, then the
    /// epoch. Its `memcmp` order is the order of [`Stamp`].
    pub fn to_key_bytes(self) -> [u8; Self::KEY_LEN] {
        let mut out = [0u8; Self::KEY_LEN];
        out[..HLC_ENCODED_LEN].copy_from_slice(&self.hlc.to_bytes());
        out[HLC_ENCODED_LEN..].copy_from_slice(&self.origin().to_bytes());
        out
    }

    /// The inverse of [`Stamp::to_key_bytes`].
    pub fn from_key_bytes(bytes: [u8; Self::KEY_LEN]) -> Self {
        let mut hlc = [0u8; HLC_ENCODED_LEN];
        hlc.copy_from_slice(&bytes[..HLC_ENCODED_LEN]);
        let mut origin = [0u8; Origin::LEN];
        origin.copy_from_slice(&bytes[HLC_ENCODED_LEN..]);
        let origin = Origin::from_bytes(origin);
        Self { hlc: Hlc::from_bytes(hlc), node: origin.node, epoch: origin.epoch }
    }

    /// The stamp as an opaque token a client can hand back.
    ///
    /// 45 bytes — a format byte (`0x01`), then the key form (the HLC's
    /// order-preserving encoding, the node id, the epoch) — in base64url without
    /// padding, the same alphabet cursors and resume tokens use. Opaque on
    /// purpose: a client compares tokens for equality and passes one back as
    /// `if_stamp`; nothing about its shape is promised, so the encoding can
    /// change without a client noticing.
    pub fn encode(&self) -> String {
        use base64::Engine as _;
        let mut raw = Vec::with_capacity(Self::TOKEN_LEN);
        raw.push(STAMP_TOKEN_FORMAT);
        raw.extend_from_slice(&self.to_key_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
    }

    /// The inverse of [`Stamp::encode`]. Refuses any other length and any first byte but the
    /// current format's.
    pub fn decode(token: &str) -> crate::Result<Self> {
        use base64::Engine as _;
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| crate::Error::MalformedStamp)?;
        let (format, key) = raw.split_first().ok_or(crate::Error::MalformedStamp)?;
        if *format != STAMP_TOKEN_FORMAT {
            return Err(crate::Error::MalformedStamp);
        }
        let key: [u8; Self::KEY_LEN] = key.try_into().map_err(|_| crate::Error::MalformedStamp)?;
        Ok(Self::from_key_bytes(key))
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
    fn predecessor_inverts_successor_and_stops_at_zero() {
        let mid = Hlc::new(1_000, 5);
        assert_eq!(mid.successor().predecessor(), mid);
        assert_eq!(Hlc::new(1_000, 0).predecessor(), Hlc::new(999, u16::MAX));
        assert_eq!(Hlc::new(999, u16::MAX).successor(), Hlc::new(1_000, 0));
        assert_eq!(Hlc::ZERO.predecessor(), Hlc::ZERO, "nothing sorts before zero");
        assert!(mid.predecessor() < mid);
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
        let lo = Stamp::new(hlc, node(1), Epoch::ZERO);
        let hi = Stamp::new(hlc, node(2), Epoch::ZERO);
        assert!(hi.wins_over(&lo));
        assert!(!lo.wins_over(&hi));
        // Reapplying an identical write must be a no-op, not a flip-flop.
        assert!(!hi.wins_over(&hi));
    }

    fn at(n: u8, e: u64) -> Stamp {
        Stamp::new(Hlc::new(100, 1), node(n), Epoch::from_u64(e))
    }

    #[test]
    fn stamp_breaks_a_node_tie_by_epoch() {
        let lo = at(4, 1);
        let hi = at(4, 2);
        assert!(hi.wins_over(&lo));
        assert!(!lo.wins_over(&hi));
        // the epoch is the last tie-break: node outranks it
        assert!(at(5, 0).wins_over(&at(4, 9)));
        assert_eq!(hi.origin(), Origin::new(node(4), Epoch::from_u64(2)));
    }

    #[test]
    fn the_key_form_is_hlc_node_epoch_in_44_bytes() {
        let s = Stamp::new(
            Hlc::new(0x0102_0304_0506_0708, 0x090A),
            NodeId::from_bytes([0xBB; 16]),
            Epoch([0xCC; 18]),
        );
        let key = s.to_key_bytes();
        assert_eq!(key.len(), 44);
        assert_eq!(Stamp::KEY_LEN, 44);
        assert_eq!(&key[..10], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(&key[10..26], &[0xBB; 16]);
        assert_eq!(&key[26..], &[0xCC; 18]);
        assert_eq!(Stamp::from_key_bytes(key), s);
    }

    #[test]
    fn the_client_token_is_45_bytes_with_a_format_byte() {
        use base64::Engine as _;
        let s = at(9, 0xABCD);
        let token = s.encode();
        assert_eq!(token.len(), 60, "45 bytes in base64url without padding");
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&token).unwrap();
        assert_eq!(raw.len(), 45);
        assert_eq!(raw[0], 0x01);
        assert_eq!(&raw[1..], &s.to_key_bytes());
        assert_eq!(Stamp::decode(&token).unwrap(), s);
    }

    #[test]
    fn the_client_token_refuses_any_other_first_byte_or_length() {
        use base64::Engine as _;
        let enc = |raw: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        let key = at(9, 5).to_key_bytes();
        let with = |first: u8| {
            let mut raw = vec![first];
            raw.extend_from_slice(&key);
            raw
        };
        assert!(Stamp::decode(&enc(&with(0x01))).is_ok());
        for first in [0x00, 0x02, 0x03, 0x80, 0xFF] {
            assert!(Stamp::decode(&enc(&with(first))).is_err(), "accepted first byte {first:#x}");
        }
        // the bare 44-byte key form, and the old 26-byte form (hlc and node)
        assert!(Stamp::decode(&enc(&key)).is_err());
        assert!(Stamp::decode(&enc(&key[..26])).is_err());
        // one byte short and one byte long, with the right first byte
        let mut raw = with(0x01);
        raw.pop();
        assert!(Stamp::decode(&enc(&raw)).is_err());
        let mut raw = with(0x01);
        raw.push(0);
        assert!(Stamp::decode(&enc(&raw)).is_err());
        assert!(Stamp::decode(&enc(&[])).is_err());
        assert!(Stamp::decode("").is_err());
        assert!(Stamp::decode("not base64 !").is_err());
        assert!(Stamp::decode(&format!("{}=", at(9, 5).encode())).is_err(), "padding is refused");
    }

    #[test]
    fn a_stamp_serdes_with_its_epoch() {
        let s = at(6, 42);
        let json = serde_json::to_value(s).unwrap();
        assert_eq!(json["epoch"], serde_json::json!(s.epoch.to_hex()));
        assert_eq!(serde_json::from_value::<Stamp>(json).unwrap(), s);
        let doc = bson::serialize_to_document(&s).unwrap();
        assert_eq!(bson::deserialize_from_document::<Stamp>(doc).unwrap(), s);
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        prop_compose! {
            fn any_stamp()(
                hlc in any_hlc(), node in any::<[u8; 16]>(), epoch in any::<[u8; 18]>()
            ) -> Stamp {
                Stamp::new(hlc, NodeId::from_bytes(node), Epoch(epoch))
            }
        }

        proptest! {
            /// The oplog is keyed by these bytes and sorted by `memcmp`: the key order must be
            /// the order of the stamps, or the window rules over the log are wrong.
            #[test]
            fn stamp_order_is_the_byte_order_of_its_key(a in any_stamp(), b in any_stamp()) {
                prop_assert_eq!(a.cmp(&b), a.to_key_bytes().cmp(&b.to_key_bytes()));
            }

            #[test]
            fn stamp_round_trips_through_key_and_token(a in any_stamp()) {
                prop_assert_eq!(Stamp::from_key_bytes(a.to_key_bytes()), a);
                prop_assert_eq!(Stamp::decode(&a.encode()).unwrap(), a);
            }
        }

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
