//! The audit log: a structured record of authorization decisions.
//!
//! # One place, because there is one decision
//!
//! Every authorization in the server funnels through
//! [`Auth::require`](crate::state::Auth::require) — REST routes, MCP tools, the
//! change-stream upgrade, the vector endpoints. The audit record is emitted
//! *there* rather than at each call site, for the same reason the check itself
//! lives there ([ADR-013](../../../docs/decisions.md)): a log that each route
//! has to remember to write is a log with holes in it, and the holes are
//! invisible.
//!
//! # Why the mode is process-global
//!
//! Auditing is a property of a deployment, not of a request, and threading it
//! through every extractor would put a configuration parameter in the signature
//! of code that has no other reason to know about configuration. It is set once
//! at startup and read atomically.
//!
//! # What is *not* audited here
//!
//! Authentication. A failed login is not an authorization decision — there is no
//! principal yet — and it is already logged and counted by the rate limiter.
//! Mixing the two would make "denied" mean two different things in one stream.
//!
//! # Volume
//!
//! `All` writes one line per authorized operation, which on a read-heavy node is
//! one line per request. That is a real cost and the reason it is not the
//! default. `Denials` is: a denial is rare, and is the event someone is actually
//! watching for.

use std::sync::atomic::{AtomicU8, Ordering};

use kimmy_auth::{Action, Principal};
use tracing::{info, warn};

/// How much to record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuditMode {
    /// Record nothing.
    Off,
    /// Record refusals only. The default: rare, and the thing worth watching.
    #[default]
    Denials,
    /// Refusals, plus anything that changed state or administered the server.
    Writes,
    /// Every decision, including reads.
    All,
}

impl AuditMode {
    fn as_u8(self) -> u8 {
        match self {
            AuditMode::Off => 0,
            AuditMode::Denials => 1,
            AuditMode::Writes => 2,
            AuditMode::All => 3,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => AuditMode::Off,
            2 => AuditMode::Writes,
            3 => AuditMode::All,
            _ => AuditMode::Denials,
        }
    }

    /// Parse the configured name.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "off" => Ok(AuditMode::Off),
            "denials" => Ok(AuditMode::Denials),
            "writes" => Ok(AuditMode::Writes),
            "all" => Ok(AuditMode::All),
            other => {
                Err(format!("unknown audit mode {other:?}; expected off, denials, writes or all"))
            }
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            AuditMode::Off => "off",
            AuditMode::Denials => "denials",
            AuditMode::Writes => "writes",
            AuditMode::All => "all",
        }
    }
}

static MODE: AtomicU8 = AtomicU8::new(1);

/// Set the process-wide audit mode. Called once, at startup.
pub fn set_mode(mode: AuditMode) {
    MODE.store(mode.as_u8(), Ordering::Relaxed);
}

pub fn mode() -> AuditMode {
    AuditMode::from_u8(MODE.load(Ordering::Relaxed))
}

/// Whether an action changes state or administers the server.
///
/// `search` and `watch` are reads: one ranks documents, the other observes
/// them. Neither writes, so neither is included at `Writes`.
fn is_write(action: Action) -> bool {
    matches!(action, Action::Write | Action::Admin)
}

/// Record one authorization decision.
///
/// Denials are `warn`, allows are `info`, and both carry the same fields so a
/// collector can treat the stream uniformly. The target is `kimmy::audit` so
/// that an operator can route the audit stream somewhere other than the
/// application log with an env-filter directive, without also capturing every
/// other event this crate emits.
pub fn record(
    principal: &Principal,
    action: Action,
    db: &str,
    collection: Option<&str>,
    allowed: bool,
) {
    let mode = mode();
    let wanted = match mode {
        AuditMode::Off => false,
        AuditMode::Denials => !allowed,
        AuditMode::Writes => !allowed || is_write(action),
        AuditMode::All => true,
    };
    if !wanted {
        return;
    }

    let collection = collection.unwrap_or("*");
    // `unauthenticated` distinguishes "root did this" from "the server was
    // started with authentication disabled", which an audit reader has to be
    // able to tell apart — the same reason the flag exists on the principal.
    if allowed {
        info!(
            target: "kimmy::audit",
            user = %principal.user,
            unauthenticated = principal.unauthenticated,
            action = ?action,
            db = %db,
            collection = %collection,
            decision = "allow",
            "authorization"
        );
    } else {
        warn!(
            target: "kimmy::audit",
            user = %principal.user,
            unauthenticated = principal.unauthenticated,
            action = ?action,
            db = %db,
            collection = %collection,
            decision = "deny",
            "authorization"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_and_round_trip() {
        for name in ["off", "denials", "writes", "all"] {
            let mode = AuditMode::parse(name).unwrap();
            assert_eq!(mode.name(), name);
            assert_eq!(AuditMode::from_u8(mode.as_u8()), mode, "the atomic encoding must survive");
        }
    }

    #[test]
    fn an_unknown_mode_lists_the_valid_ones() {
        let err = AuditMode::parse("verbose").unwrap_err();
        assert!(err.contains("verbose"), "{err}");
        assert!(err.contains("denials"), "the error should say what is valid: {err}");
    }

    #[test]
    fn the_default_records_denials() {
        // A default of `all` would put one line per request on a read-heavy
        // node; a default of `off` would mean nobody gets the one event they
        // actually want. Denials are rare and are what someone is watching for.
        assert_eq!(AuditMode::default(), AuditMode::Denials);
    }

    #[test]
    fn search_and_watch_are_reads_not_writes() {
        // At `writes`, an audit reader is asking "what changed". Ranking
        // documents and observing them do not.
        assert!(!is_write(Action::Search));
        assert!(!is_write(Action::Watch));
        assert!(!is_write(Action::Read));
        assert!(is_write(Action::Write));
        assert!(is_write(Action::Admin));
    }
}
