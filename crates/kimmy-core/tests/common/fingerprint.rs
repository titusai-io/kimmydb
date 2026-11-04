//! A deterministic fingerprint of merge states, for the tests that must show a change to the
//! `merge_state` code left every state, every byte and every read unchanged.
//!
//! A [`Fp`] folds, for a state WITHOUT its order state (see [`without_ords`]), its `state_hash` and `encode` bytes (where the version 1 form can
//! hold the state), its compact bytes and hash, the visible document, the hidden figures, the
//! excess and the caps, as bytes into a 64-bit FNV-1a. It is a change detector, not a format: the
//! pinned values in the tests are the figures of the code before the performance pass of Q7 step 0.

#![allow(dead_code)]

use kimmy_core::merge_state::{JoinOptions, MergeState};

/// The state without its order state. NAMED RESTRICTION (item O2): the vendored stage-A model, which
/// the differentials compare with, has no order state, so the bytes, the hashes and the fingerprints
/// of the comparison leave the Ord state out; the Ord state is compared with the A2.1 model in its
/// own differential (`merge_state_ord_diff`). Lifted at the D8 re-vendor.
pub fn without_ords(s: &MergeState) -> MergeState {
    let mut s = s.clone();
    s.ords.clear();
    s
}

pub struct Fp(u64);

impl Default for Fp {
    fn default() -> Self {
        Self::new()
    }
}

impl Fp {
    pub fn new() -> Self {
        Fp(0xcbf2_9ce4_8422_2325)
    }

    /// Fold bytes, length first so that two folds cannot run together.
    pub fn bytes(&mut self, b: &[u8]) {
        self.raw(&(b.len() as u64).to_be_bytes());
        self.raw(b);
    }

    fn raw(&mut self, b: &[u8]) {
        for x in b {
            self.0 ^= u64::from(*x);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }

    pub fn text(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    /// Everything a state and its reads show.
    pub fn state(&mut self, s: &MergeState, o: &JoinOptions) {
        let s = &without_ords(s);
        match s.try_encode() {
            Ok(e) => {
                self.bytes(&e);
                self.bytes(&s.try_state_hash().expect("version 1 spells the states of the runs"));
            }
            Err(e) => self.text(&format!("{e:?}")),
        }
        self.bytes(&s.compact_bytes());
        self.bytes(&s.compact_hash());
        self.text(&format!("{:?}", s.visible(o)));
        self.text(&format!("{:?}", s.hidden_all(o)));
        self.text(&format!("{:?}", s.excess(o)));
        self.text(&format!("{:?}", s.over_caps(o)));
        self.text(&format!("{:?}", s.alive(o)));
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}
