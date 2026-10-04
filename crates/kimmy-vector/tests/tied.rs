//! The numbers the yield evaluator's bounds are tied to (ADR-213, section 6.3).
//!
//! `kimmy_cluster::yielding` derives the embedding class's `Waiting` and `Remote`
//! bounds from the worker's own ticks and the provider's attempt timeout, and holds
//! a copy of each in `yielding::tied`. A change here that would loosen a bound
//! must fail to build, so each is asserted equal at compile time, and again as a
//! test so the failure names the pair.

use std::time::Duration;

use kimmy_cluster::yielding::tied;
use kimmy_vector::provider::REQUEST_TIMEOUT;
use kimmy_vector::worker::{DEFERRAL_TICK, OWNERSHIP_TICK, RETRY_DELAY};

const fn same(a: Duration, b: Duration) -> bool {
    a.as_millis() == b.as_millis()
}

const _: () = assert!(same(OWNERSHIP_TICK, tied::OWNERSHIP_TICK));
const _: () = assert!(same(DEFERRAL_TICK, tied::DEFERRAL_TICK));
const _: () = assert!(same(RETRY_DELAY, tied::RETRY_DELAY));
const _: () = assert!(same(REQUEST_TIMEOUT, tied::ATTEMPT_TIMEOUT));

#[test]
fn each_tied_constant_equals_its_twin_in_the_evaluator() {
    assert_eq!(OWNERSHIP_TICK, tied::OWNERSHIP_TICK, "the worker's ownership tick");
    assert_eq!(DEFERRAL_TICK, tied::DEFERRAL_TICK, "the worker's deferral tick");
    assert_eq!(RETRY_DELAY, tied::RETRY_DELAY, "the worker's retry delay");
    assert_eq!(REQUEST_TIMEOUT, tied::ATTEMPT_TIMEOUT, "one provider attempt's timeout");
}
