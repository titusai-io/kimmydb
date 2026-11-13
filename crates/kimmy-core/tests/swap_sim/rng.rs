//! Deterministic seeded xorshift64*; no `rand`. A copy of `merge_state_sim/rng.rs` with a draw recorder for the Python replay
//! (`Q6` draw export: `B n v` for `below`, `F x` for `f64`, `P a,b,c` for a shuffle of page names, written by `snapshot::shuffle`).

use std::cell::RefCell;

thread_local! {
    /// The recorded draws of this thread, while a recording is on.
    pub static LOG: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
    /// While set, draws are not recorded (the inside of a recorded shuffle).
    pub static MUTE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn record(line: String) {
    if MUTE.with(|m| m.get()) {
        return;
    }
    LOG.with(|l| {
        if let Some(v) = l.borrow_mut().as_mut() {
            v.push(line);
        }
    });
}

#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // splitmix64 the seed so that small seeds give unrelated streams and the state is never 0.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        Rng(if z == 0 { 0x2545_F491_4F6C_DD1D } else { z })
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `[0, n)`.
    pub fn below(&mut self, n: usize) -> usize {
        let v = ((self.next_u64() >> 11) % n as u64) as usize;
        record(format!("B {n} {v}"));
        v
    }

    /// Uniform in `[0, 1)`.
    pub fn f64(&mut self) -> f64 {
        let v = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        record(format!("F {v:?}"));
        v
    }

    pub fn choice<T: Clone>(&mut self, xs: &[T]) -> T {
        xs[self.below(xs.len())].clone()
    }

    /// Two distinct indices below `n` (`random.sample(ms, 2)`).
    pub fn sample2(&mut self, n: usize) -> (usize, usize) {
        let a = self.below(n);
        let mut b = self.below(n - 1);
        if b >= a {
            b += 1;
        }
        (a, b)
    }
}
