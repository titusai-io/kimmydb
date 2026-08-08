//! Request rate limiting.
//!
//! A token bucket per key, with physical time passed in as a parameter
//! ([ADR-007](../../../docs/decisions.md)). That is what makes the behaviour
//! that matters here — refill across a window, a bucket that recovers, a clock
//! that jumps backwards — an ordinary unit test rather than a test that sleeps.
//!
//! # Why login is the route that has one
//!
//! Everywhere else in the API a limit would be a *capacity* control, and
//! capacity numbers picked without measurement are guesses. On
//! `/v1/auth/login` it is a *security* control, for two reasons:
//!
//! - passwords are guessable at network speed, and nothing else stands in the
//!   way — the endpoint is unauthenticated by necessity;
//! - every attempt runs a full Argon2id verification, **including for a user
//!   that does not exist** (`kimmy_auth::UserStore::authenticate` hashes anyway,
//!   so a missing user cannot be distinguished by timing). At the configured
//!   work factor that is ~19 MB and a couple of milliseconds of CPU per
//!   request, which makes an unthrottled login endpoint an amplifier.
//!
//! The second reason is why [`Limiter::check_at`] is separate from
//! [`Limiter::record_at`]: the decision has to be made *before* the hash runs,
//! or the limit does not prevent the work it exists to prevent.
//!
//! # Why only failures are recorded
//!
//! A caller presenting correct credentials is not the thing being defended
//! against, and a client that legitimately re-authenticates often — a fleet
//! restarting, a short `token_ttl_secs` — must not be throttled for it. So the
//! handler consumes a token when authentication *fails* and leaves the bucket
//! alone when it succeeds. A limit that punished success would be a capacity
//! control wearing a security control's clothes.
//!
//! # Reusing this elsewhere
//!
//! [`Limiter`] knows nothing about login; it maps an arbitrary key to a budget.
//! Applying it to another route is a `Limiter` in [`RateLimits`], a knob in the
//! config, and a `check_at` call — either in the handler, when the key depends
//! on the body, or in a `tower` layer when the key is just the caller.

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::Mutex;

use crate::error::ApiError;

/// How many attempts are allowed, and over what period they are replenished.
///
/// Expressed as a burst and a window rather than a rate per second because that
/// is how an operator thinks about it — "ten tries a minute" — and because the
/// burst is the part that matters for a bucket that starts full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimit {
    /// Attempts available at once, and the bucket's capacity.
    pub burst: u32,
    /// The period over which a fully drained bucket refills completely.
    pub window: Duration,
}

impl RateLimit {
    pub const fn new(burst: u32, window: Duration) -> Self {
        Self { burst, window }
    }

    /// A limit of zero burst is how a limiter is turned off, so that disabling
    /// one is a config value rather than a second flag that can disagree with
    /// it.
    pub const fn is_disabled(&self) -> bool {
        self.burst == 0
    }

    /// Tokens replenished per millisecond.
    fn refill_per_ms(&self) -> f64 {
        let window_ms = self.window.as_millis().max(1) as f64;
        f64::from(self.burst) / window_ms
    }
}

/// The answer to "may this caller proceed".
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Decision {
    Allowed,
    /// Refused, with how long until one token is available again. Reported to
    /// the caller as `Retry-After` so a well-behaved client backs off by the
    /// right amount instead of hammering or giving up.
    Limited {
        retry_after: Duration,
    },
}

impl Decision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allowed)
    }
}

#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    /// When `tokens` was last correct. Refill is computed lazily from this
    /// rather than by a background task, so an idle key costs nothing.
    updated_ms: u64,
}

/// A token bucket per key.
///
/// Keys are owned `String`s rather than borrowed, because the useful ones — a
/// client address, a username — are built per request anyway.
pub struct Limiter {
    limit: RateLimit,
    /// The cap exists because the key space is attacker-controlled: a source
    /// address is whatever packets arrive from, and a username is whatever was
    /// typed. Without a bound, the defence against one denial of service is
    /// itself one.
    max_keys: usize,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl Limiter {
    pub fn new(limit: RateLimit, max_keys: usize) -> Self {
        Self { limit, max_keys: max_keys.max(1), buckets: Mutex::new(HashMap::new()) }
    }

    pub fn limit(&self) -> RateLimit {
        self.limit
    }

    /// Would this key be allowed right now? Does not consume anything.
    pub fn check(&self, key: &str) -> Decision {
        self.check_at(now_ms(), key)
    }

    /// Consume one token for this key.
    pub fn record(&self, key: &str) {
        self.record_at(now_ms(), key);
    }

    /// [`Limiter::check`] against a supplied clock.
    ///
    /// Peeking rather than consuming means two requests arriving in the same
    /// instant can both be allowed against a single remaining token. That is
    /// deliberate: the alternative couples the decision to the outcome, and the
    /// bucket drains on the very next failure anyway, so the overshoot is one
    /// round of concurrency and not a hole.
    pub fn check_at(&self, now_ms: u64, key: &str) -> Decision {
        if self.limit.is_disabled() {
            return Decision::Allowed;
        }

        let mut buckets = self.buckets.lock();
        // An absent key has never been recorded against, so it has a full
        // bucket by definition — no entry is created, or a bare `check` would
        // let anyone fill the map without ever failing a login.
        let Some(bucket) = buckets.get_mut(key) else {
            return Decision::Allowed;
        };

        self.refill(bucket, now_ms);
        if bucket.tokens >= 1.0 {
            Decision::Allowed
        } else {
            Decision::Limited { retry_after: self.wait_for_one_token(bucket.tokens) }
        }
    }

    /// [`Limiter::record`] against a supplied clock.
    pub fn record_at(&self, now_ms: u64, key: &str) {
        if self.limit.is_disabled() {
            return;
        }

        // Scoped so the guard is released before eviction, which takes the same
        // lock. `parking_lot::Mutex` is not reentrant — holding across the call
        // would deadlock rather than fail.
        {
            let mut buckets = self.buckets.lock();
            if let Some(bucket) = buckets.get_mut(key) {
                self.refill(bucket, now_ms);
                // Floored at zero rather than allowed to go negative: a long
                // burst should not extend the penalty past the window, or the
                // configured number stops meaning what it says.
                bucket.tokens = (bucket.tokens - 1.0).max(0.0);
                return;
            }
        }

        self.evict_if_full(now_ms);
        let full = f64::from(self.limit.burst);
        self.buckets
            .lock()
            .insert(key.to_string(), Bucket { tokens: full - 1.0, updated_ms: now_ms });
    }

    /// Number of keys currently tracked. Exposed for tests and for a future
    /// metric; the count is the thing that tells an operator an attack is on.
    pub fn tracked_keys(&self) -> usize {
        self.buckets.lock().len()
    }

    fn refill(&self, bucket: &mut Bucket, now_ms: u64) {
        // Saturating, so a clock that jumps backwards leaves the bucket where
        // it was instead of producing a negative elapsed time and, through the
        // `as f64` cast, an enormous refill that would clear the limit. NTP
        // steps backwards; a limiter that can be reset by one is not a limiter.
        let elapsed = now_ms.saturating_sub(bucket.updated_ms) as f64;
        bucket.tokens =
            (bucket.tokens + elapsed * self.limit.refill_per_ms()).min(f64::from(self.limit.burst));
        bucket.updated_ms = now_ms;
    }

    fn wait_for_one_token(&self, tokens: f64) -> Duration {
        let needed = (1.0 - tokens).max(0.0);
        let ms = (needed / self.limit.refill_per_ms()).ceil();
        // At least a second, so `Retry-After: 0` never tells a client to retry
        // immediately against a limit it has just hit.
        Duration::from_millis((ms as u64).max(1_000))
    }

    /// Make room, if the map is at its cap.
    ///
    /// Buckets that have refilled completely carry no information and go first.
    /// If that is not enough, the fullest remaining bucket is dropped: it is the
    /// one with the least evidence of abuse against it, so forgetting it costs
    /// the least. An attacker cycling through addresses can still push entries
    /// out — but the memory stays bounded, which is the property being defended.
    fn evict_if_full(&self, now_ms: u64) {
        let mut buckets = self.buckets.lock();
        if buckets.len() < self.max_keys {
            return;
        }

        let capacity = f64::from(self.limit.burst);
        let rate = self.limit.refill_per_ms();
        buckets.retain(|_, bucket| {
            let elapsed = now_ms.saturating_sub(bucket.updated_ms) as f64;
            (bucket.tokens + elapsed * rate) < capacity
        });

        while buckets.len() >= self.max_keys {
            let fullest = buckets
                .iter()
                .max_by(|a, b| {
                    let a_tokens = a.1.tokens + now_ms.saturating_sub(a.1.updated_ms) as f64 * rate;
                    let b_tokens = b.1.tokens + now_ms.saturating_sub(b.1.updated_ms) as f64 * rate;
                    a_tokens.total_cmp(&b_tokens)
                })
                .map(|(key, _)| key.clone());

            match fullest {
                Some(key) => {
                    buckets.remove(&key);
                }
                // Unreachable while `max_keys >= 1`, but looping forever on an
                // empty map would be a worse way to find that out.
                None => break,
            }
        }
    }
}

/// The limiters the server holds.
///
/// A struct rather than a map keyed by name so that adding a limit is a
/// compile-time change: a route that reaches for one that does not exist should
/// not be a runtime `None` that silently means "unlimited".
pub struct RateLimits {
    /// Failed logins per client address.
    pub login_ip: Limiter,
    /// Failed logins per attempted username, across all addresses.
    ///
    /// Off by default. It is the only defence against a brute force spread over
    /// many source addresses, but it introduces a lockout: anyone who can reach
    /// the endpoint can exhaust a *named* user's budget and keep a legitimate
    /// holder of that name out. Enabling it trades one denial of service for
    /// another, which is an operator's call to make knowingly rather than a
    /// default to inherit.
    pub login_user: Limiter,
    /// Header naming the real client when the server sits behind a proxy.
    ///
    /// Empty means "use the socket peer address". This is opt-in because a
    /// forwarded header is client-supplied data: trusting one by default would
    /// let anyone defeat per-address limiting by varying a header, which is
    /// strictly worse than having no limiter at all — it would look like it was
    /// working.
    pub trusted_proxy_header: Option<String>,
}

impl RateLimits {
    /// Every limiter disabled. Used where a limit would only get in the way —
    /// unit tests, and `--insecure-no-auth`, which has no login to protect.
    pub fn disabled() -> Self {
        let off = || Limiter::new(RateLimit::new(0, Duration::from_secs(1)), 1);
        Self { login_ip: off(), login_user: off(), trusted_proxy_header: None }
    }
}

/// Refuse a request that is over its limit.
pub fn too_many_requests(retry_after: Duration) -> ApiError {
    ApiError::too_many_requests(retry_after.as_secs().max(1))
}

/// Wall-clock milliseconds.
///
/// The only place in this module that reads a clock, so every rule above is
/// reachable from a test that does not sleep.
fn now_ms() -> u64 {
    kimmy_storage::physical_now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: u64 = 1_700_000_000_000;

    fn limiter(burst: u32, window_secs: u64) -> Limiter {
        Limiter::new(RateLimit::new(burst, Duration::from_secs(window_secs)), 1024)
    }

    #[test]
    fn an_unseen_key_is_allowed_without_being_tracked() {
        // Otherwise a bare check is itself a way to fill the map.
        let limiter = limiter(3, 60);
        assert!(limiter.check_at(T0, "10.0.0.1").is_allowed());
        assert_eq!(limiter.tracked_keys(), 0, "checking must not allocate a bucket");
    }

    #[test]
    fn the_burst_is_spent_before_the_limit_applies() {
        let limiter = limiter(3, 60);
        for attempt in 0..3 {
            assert!(
                limiter.check_at(T0, "10.0.0.1").is_allowed(),
                "attempt {attempt} is within the burst of 3"
            );
            limiter.record_at(T0, "10.0.0.1");
        }
        assert!(
            !limiter.check_at(T0, "10.0.0.1").is_allowed(),
            "the fourth attempt is past a burst of 3"
        );
    }

    #[test]
    fn keys_do_not_share_a_budget() {
        // The whole point of keying: one exhausted caller must not lock out
        // every other caller.
        let limiter = limiter(1, 60);
        limiter.record_at(T0, "10.0.0.1");
        assert!(!limiter.check_at(T0, "10.0.0.1").is_allowed());
        assert!(limiter.check_at(T0, "10.0.0.2").is_allowed());
    }

    #[test]
    fn a_bucket_refills_across_its_window() {
        let limiter = limiter(10, 60);
        for _ in 0..10 {
            limiter.record_at(T0, "k");
        }
        assert!(!limiter.check_at(T0, "k").is_allowed());

        // A tenth of the window returns a tenth of the burst: one token.
        assert!(
            limiter.check_at(T0 + 6_000, "k").is_allowed(),
            "6s of a 60s window must return one of ten tokens"
        );
    }

    #[test]
    fn refill_never_exceeds_the_burst() {
        // Otherwise an idle key banks credit and the burst stops being a cap —
        // a client quiet for an hour could then spend an hour's worth at once.
        let limiter = limiter(5, 60);
        limiter.record_at(T0, "k");
        for _ in 0..5 {
            assert!(limiter.check_at(T0 + 3_600_000, "k").is_allowed());
            limiter.record_at(T0 + 3_600_000, "k");
        }
        assert!(
            !limiter.check_at(T0 + 3_600_000, "k").is_allowed(),
            "an hour idle must still only buy a burst of 5"
        );
    }

    #[test]
    fn a_backwards_clock_does_not_clear_the_limit() {
        // NTP steps backwards. Subtracting a later timestamp from an earlier
        // one would underflow to an enormous elapsed time and refill the bucket
        // completely, which is a limiter an attacker resets by waiting for a
        // clock correction.
        let limiter = limiter(2, 60);
        limiter.record_at(T0, "k");
        limiter.record_at(T0, "k");
        assert!(!limiter.check_at(T0, "k").is_allowed());

        assert!(
            !limiter.check_at(T0 - 3_600_000, "k").is_allowed(),
            "a clock that jumped backwards must not grant credit"
        );
    }

    #[test]
    fn retry_after_is_how_long_until_a_token_returns() {
        let limiter = limiter(10, 600);
        for _ in 0..10 {
            limiter.record_at(T0, "k");
        }

        // One of ten tokens over a 600s window is 60s.
        match limiter.check_at(T0, "k") {
            Decision::Limited { retry_after } => {
                assert_eq!(retry_after, Duration::from_secs(60), "one token of ten over 600s");
            }
            Decision::Allowed => panic!("must be limited immediately after draining the bucket"),
        }
    }

    #[test]
    fn retry_after_is_never_zero() {
        // `Retry-After: 0` tells a client to retry instantly against a limit it
        // has just hit, which is the opposite of backing off.
        let limiter = limiter(1000, 1);
        for _ in 0..1000 {
            limiter.record_at(T0, "k");
        }
        match limiter.check_at(T0, "k") {
            Decision::Limited { retry_after } => assert!(retry_after >= Duration::from_secs(1)),
            Decision::Allowed => panic!("expected to be limited"),
        }
    }

    #[test]
    fn a_disabled_limiter_allows_everything_and_stores_nothing() {
        let limiter = Limiter::new(RateLimit::new(0, Duration::from_secs(60)), 1024);
        for _ in 0..1000 {
            assert!(limiter.check_at(T0, "k").is_allowed());
            limiter.record_at(T0, "k");
        }
        assert_eq!(limiter.tracked_keys(), 0, "a disabled limiter must not accumulate state");
    }

    #[test]
    fn the_tracked_key_count_stays_bounded() {
        // The key space is attacker-controlled — an address is whatever packets
        // arrive from. Unbounded growth would make the defence a denial of
        // service in its own right.
        let limiter = Limiter::new(RateLimit::new(5, Duration::from_secs(600)), 32);
        for i in 0..10_000 {
            limiter.record_at(T0, &format!("10.0.{}.{}", i / 256, i % 256));
        }
        assert!(
            limiter.tracked_keys() <= 32,
            "expected the map capped at 32, found {}",
            limiter.tracked_keys()
        );
    }

    #[test]
    fn eviction_prefers_keys_that_have_recovered() {
        // A fully refilled bucket carries no information; a drained one is the
        // record of an attack in progress and is the last thing to forget.
        let limiter = Limiter::new(RateLimit::new(4, Duration::from_secs(600)), 4);

        // Three keys spend one token each and are then left alone for a full
        // window, so by `later` they have refilled completely.
        for key in ["a", "b", "c"] {
            limiter.record_at(T0, key);
        }
        let later = T0 + 600_000;

        // A fourth drains its bucket at `later` and so is still empty.
        for _ in 0..4 {
            limiter.record_at(later, "attacker");
        }
        assert_eq!(limiter.tracked_keys(), 4, "the map should now be at its cap");

        // A fifth key forces an eviction.
        limiter.record_at(later, "e");

        assert!(
            !limiter.check_at(later, "attacker").is_allowed(),
            "the drained bucket must survive eviction; it is the only one still carrying \
             information, and forgetting it would clear the limit for the caller under attack"
        );
    }
}
