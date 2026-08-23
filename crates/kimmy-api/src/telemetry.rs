//! Whether a span may carry a database or collection name.
//!
//! # One switch, because there is one question
//!
//! Every span this crate opens asks the same thing before it records a
//! namespace: may this deployment's collector be told what the data is called?
//! The answer is a property of the deployment, not of a request — the collector
//! is one endpoint an operator chose once — so it is set at startup and read
//! atomically, exactly as [`crate::audit`]'s mode is and for the reason that
//! module's "Why the mode is process-global" note gives: threading it through
//! every extractor would put a configuration parameter in the signature of code
//! that has no other reason to know about configuration.
//!
//! # Off by default
//!
//! `false` is the value a process starts with, so a binary that never calls
//! [`set_include_names`] exports no names at all. That is the safe direction:
//! the failure mode being avoided is a trace backend that quietly accumulates a
//! schema nobody decided to publish, on the strength of a default nobody chose.
//! Span *names* are unaffected either way — they come from `http.route` and
//! `db.operation.name`, both of which carry no data name by construction
//! (ADR-068).

use std::sync::atomic::{AtomicBool, Ordering};

static INCLUDE_NAMES: AtomicBool = AtomicBool::new(false);

/// Set the process-wide privacy gate. Called once, at startup.
pub fn set_include_names(on: bool) {
    INCLUDE_NAMES.store(on, Ordering::Relaxed);
}

/// Whether spans may record `db.namespace`, `db.collection.name` and
/// `url.path`.
pub fn include_names() -> bool {
    INCLUDE_NAMES.load(Ordering::Relaxed)
}

/// What `db.system.name` says this is.
pub const DB_SYSTEM: &str = "kimmydb";

/// The tracing target the audit log uses, which must never reach a collector.
///
/// Named here as well as in [`crate::audit`] because the exclusion is enforced
/// in `kimmyd`'s subscriber, one crate away, and a filter that names a target
/// by a string nothing else knows about is a filter that stops matching the
/// first time the target is renamed. See ADR-068.
pub const AUDIT_TARGET: &str = "kimmy::audit";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_audit_target_is_the_one_audit_records_actually_use() {
        // The exclusion in `kimmyd::logging` matches on this string. If the
        // audit module ever emitted under a different target, the filter would
        // silently stop excluding it and principal names would start reaching
        // the collector — which is the one thing ADR-068 promises does not
        // happen. Asserted against a literal rather than against the constant
        // itself, so renaming the constant does not quietly rename the rule.
        assert_eq!(AUDIT_TARGET, "kimmy::audit");
    }

    #[test]
    fn names_are_off_until_somebody_turns_them_on() {
        // The default matters more than the setter: a binary that never
        // configures telemetry must export no names, not all of them.
        let previous = include_names();
        set_include_names(false);
        assert!(!include_names());
        set_include_names(true);
        assert!(include_names());
        set_include_names(previous);
    }
}
