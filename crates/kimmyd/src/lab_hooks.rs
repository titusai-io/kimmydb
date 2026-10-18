//! The lab hooks of the B5 cluster tests: a member that behaves as a release
//! before 0.52 does, and a keyed `multi` request held between two chunks. Built
//! only with the `lab-hooks` cargo feature; the shipped binary has none of it (the
//! release check is `strings kimmyd | grep -c` of each switch's name reading 0).
//!
//! ```text
//! KIMMY_TEST_STRIP_RECORDS=1
//! KIMMY_TEST_HOLD_BETWEEN_CHUNKS=<path>
//! ```
//!
//! `KIMMY_TEST_STRIP_RECORDS=1` makes this member serve every window, push,
//! replay and snapshot page **without** idempotency records, and leaves
//! `idempotency_records` out of its facts block, as a member of an older release
//! does: the in-process mixed-version test of the design (C9). What it receives
//! it stores as this build does.
//!
//! `KIMMY_TEST_HOLD_BETWEEN_CHUNKS=<path>` holds the first request of this process
//! that goes on past its first commit (a `multi` write between two chunks,
//! ADR-192) until `<path>` exists, for at most two minutes: a test places a
//! competing write there. The request's thread waits; the writer is free.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use kimmy_storage::Engine;
use tracing::warn;

const STRIP_RECORDS: &str = "KIMMY_TEST_STRIP_RECORDS";
const HOLD_BETWEEN_CHUNKS: &str = "KIMMY_TEST_HOLD_BETWEEN_CHUNKS";

/// The longest a held request waits for its release file.
const HOLD_AT_MOST: Duration = Duration::from_secs(120);

/// Whether `KIMMY_TEST_STRIP_RECORDS` is set to `1`: read once per process.
pub(crate) fn strips_records() -> bool {
    static STRIPS: OnceLock<bool> = OnceLock::new();
    *STRIPS.get_or_init(|| std::env::var(STRIP_RECORDS).is_ok_and(|value| value == "1"))
}

/// Arm what the environment asks for, on the store just opened.
pub(crate) fn arm(engine: &Arc<Engine>) {
    if strips_records() {
        kimmy_cluster::transport::set_test_strip_records(vec![engine.node_id()]);
        warn!(
            switch = STRIP_RECORDS,
            "this member serves without idempotency records and its block omits \
             idempotency_records, as a release before 0.52 does"
        );
    }
    if let Ok(path) = std::env::var(HOLD_BETWEEN_CHUNKS) {
        let path = std::path::PathBuf::from(path);
        engine.before_next_continuing_write(move || {
            warn!(
                switch = HOLD_BETWEEN_CHUNKS,
                path = %path.display(),
                "holding a request between two chunks until the path exists"
            );
            let since = Instant::now();
            while !path.exists() && since.elapsed() < HOLD_AT_MOST {
                std::thread::sleep(Duration::from_millis(20));
            }
            warn!(switch = HOLD_BETWEEN_CHUNKS, "released the request held between two chunks");
        });
    }
}
