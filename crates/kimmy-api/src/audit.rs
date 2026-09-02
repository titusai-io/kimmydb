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
    matches!(action, Action::Write | Action::Ddl | Action::Admin)
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
    // The roles held, not "the role that decided". Grants are a union and more
    // than one role can supply the same permission, so a single deciding role
    // is not well defined — which one `can` happened to match first is an
    // implementation detail, not a fact worth putting in an audit record.
    //
    // It matters most for a federated caller: there is no user record here to
    // read the answer back from later, so this line is the only place that
    // association is ever recoverable.
    let roles = principal.roles.join(",");
    // The readable name, only when it adds something: a federated subject is
    // opaque, and `display=ada@example.com` beside `user=3f2a…` is what lets
    // the person reading this line know who that was without opening the
    // provider's console. Omitted when it would repeat `user`, and omitted for
    // every local principal, so the field's presence itself says "this came
    // from the provider's claim". It is presentation: `user` is the identity
    // the decision was made on, and this is never used to make one (ADR-100).
    //
    // An `Option` field records nothing when it is `None`, which is how the
    // field is absent rather than empty; wrapped as a `Display` value so it
    // prints unquoted like `user` does. (Not named `display` locally: the
    // macro brings `tracing::field::display` into scope under that name.)
    let display_name = principal.display.as_deref().filter(|d| *d != principal.user);
    // `unauthenticated` distinguishes "root did this" from "the server was
    // started with authentication disabled", and `federated` distinguishes
    // both from "somebody the identity provider called root did this" — which
    // an audit reader has to be able to tell apart, because a name alone
    // cannot: nothing stops an IdP from asserting a subject that matches a
    // local account. Same reason the flags exist on the principal.
    if allowed {
        info!(
            target: "kimmy::audit",
            user = %principal.user,
            display = display_name.map(tracing::field::display),
            unauthenticated = principal.unauthenticated,
            federated = principal.federated,
            roles = %roles,
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
            display = display_name.map(tracing::field::display),
            unauthenticated = principal.unauthenticated,
            federated = principal.federated,
            roles = %roles,
            action = ?action,
            db = %db,
            collection = %collection,
            decision = "deny",
            "authorization"
        );
    }
}

/// Record that a collection's embedding provider was configured or removed.
///
/// Beside the authorization record rather than inside it, as the webhook
/// records are: `Auth::require` already wrote that `ddl` was allowed, and
/// what this adds is *what the grant was used for* — which provider, at which
/// host or under which profile, reading which variable. That is the fact an
/// audit reader needs when asking where a collection's text has been sent
/// and with what credential (ADR-115). Always the variable's *name*, never a
/// value: the value is read by the provider at use time and appears nowhere.
///
/// Unconditional, like the webhook records: the audit mode governs the volume
/// of authorization decisions, and a change to where the node sends data is
/// rare and always worth a line.
pub fn record_vectors(
    principal: &Principal,
    action: &'static str,
    db: &str,
    collection: &str,
    provider: Option<&kimmy_core::ProviderConfig>,
) {
    // The host rather than the URL: a path can carry a deployment name or a
    // query string, and the destination is the fact worth recording.
    let host = provider.and_then(|p| p.endpoint()).and_then(kimmy_egress::host_of);
    let profile = provider.and_then(|p| match p {
        kimmy_core::ProviderConfig::Profile { name } => Some(name.as_str()),
        _ => None,
    });
    let api_key_env = provider.and_then(|p| p.api_key_env());
    // Display rather than Debug for the string fields, as `user` is, so the
    // line reads `api_key_env=OPENAI_API_KEY` and not a quoted literal; an
    // absent `Option` records nothing, which is how a field is absent rather
    // than empty.
    warn!(
        target: "kimmy::audit",
        user = %principal.user,
        unauthenticated = principal.unauthenticated,
        federated = principal.federated,
        action = %action,
        db = %db,
        collection = %collection,
        provider = provider.map(|p| tracing::field::display(p.name())),
        endpoint_host = host.map(tracing::field::display),
        profile = profile.map(tracing::field::display),
        api_key_env = api_key_env.map(tracing::field::display),
        decision = "allow",
        "embedding provider changed"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kimmy_auth::Grant;
    use parking_lot::Mutex;

    use super::*;

    /// A writer a test can read back, so an assertion can be made about the
    /// record's fields rather than about the call that produced them.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// The audit lines one decision produces, at `all`.
    fn record_of(principal: &Principal) -> String {
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .without_time()
            .with_ansi(false)
            .finish();
        // Scoped to this thread, so a parallel test's mode does not leak in;
        // the mode itself is process-global, hence set inside the scope.
        tracing::subscriber::with_default(subscriber, || {
            let previous = mode();
            set_mode(AuditMode::All);
            record(principal, Action::Read, "sales", Some("orders"), true);
            set_mode(previous);
        });
        let out = sink.0.lock().clone();
        String::from_utf8(out).expect("utf-8")
    }

    #[test]
    fn the_record_says_where_the_identity_came_from() {
        // Three origins, and a name cannot separate them: nothing stops an
        // identity provider from asserting a subject called "root". An audit
        // reader has to be able to tell "root did this" from "somebody the IdP
        // called root did this" from "the server was started with
        // authentication off".
        let local = record_of(&Principal::superuser("root"));
        assert!(local.contains("user=root"), "{local}");
        assert!(local.contains("unauthenticated=false"), "{local}");
        assert!(local.contains("federated=false"), "{local}");

        let federated = record_of(&Principal::federated(
            "root",
            vec![Grant::new("sales", "orders", vec![Action::Read])],
        ));
        assert!(federated.contains("user=root"), "{federated}");
        assert!(federated.contains("federated=true"), "{federated}");
        assert!(federated.contains("unauthenticated=false"), "{federated}");

        let no_auth = record_of(&Principal::insecure_root());
        assert!(no_auth.contains("unauthenticated=true"), "{no_auth}");
        assert!(no_auth.contains("federated=false"), "{no_auth}");
    }

    #[test]
    fn the_display_name_appears_only_when_it_says_something_the_user_does_not() {
        // The federated subject from a real provider is opaque; the display
        // is what a person recognises. It rides beside `user`, never in place
        // of it — the identity the decision was made on stays on the line.
        let named = record_of(
            &Principal::federated("3f2a9c1e", vec![]).with_display(Some("ada@example.com".into())),
        );
        assert!(named.contains("user=3f2a9c1e"), "{named}");
        assert!(named.contains("display=ada@example.com"), "{named}");

        // Absent, not empty, when there is nothing to add: a local principal,
        // and a federated one whose claim was missing or merely repeated the
        // subject. A collector keyed on the field's presence can then read
        // "this came from the provider's claim" from it.
        for principal in [
            Principal::superuser("root"),
            Principal::federated("ada", vec![]),
            Principal::federated("ada", vec![]).with_display(Some("ada".into())),
        ] {
            let line = record_of(&principal);
            assert!(!line.contains("display"), "{line}");
        }
    }

    /// The audit lines one vector record produces.
    fn vectors_record_of(
        action: &'static str,
        provider: Option<&kimmy_core::ProviderConfig>,
    ) -> String {
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .without_time()
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            record_vectors(&Principal::superuser("root"), action, "shop", "docs", provider);
        });
        String::from_utf8(sink.0.lock().clone()).expect("utf-8")
    }

    #[test]
    fn a_vector_configuration_is_recorded_with_its_destination_and_key_name() {
        // What an audit reader asks after ADR-115: where has this
        // collection's text been sent, and which variable authenticated it.
        // The host and the variable's *name*; the URL's path and the value
        // are not facts an audit line should hold.
        let openai = kimmy_core::ProviderConfig::OpenAi {
            model: "text-embedding-3-small".into(),
            endpoint: Some("https://api.voyageai.com/v1/embeddings?deployment=secret".into()),
            api_key_env: "KIMMY_PROVIDER_VOYAGE".into(),
            dimensions: None,
        };
        let line = vectors_record_of("ConfigureVectors", Some(&openai));
        assert!(line.contains("kimmy::audit"), "{line}");
        assert!(line.contains("action=ConfigureVectors"), "{line}");
        assert!(line.contains("user=root"), "{line}");
        assert!(line.contains("db=shop"), "{line}");
        assert!(line.contains("collection=docs"), "{line}");
        assert!(line.contains("provider=openai"), "{line}");
        assert!(line.contains("endpoint_host=api.voyageai.com"), "{line}");
        assert!(line.contains("api_key_env=KIMMY_PROVIDER_VOYAGE"), "{line}");
        assert!(line.contains("decision=\"allow\""), "{line}");
        assert!(!line.contains("deployment=secret"), "the path is not recorded: {line}");
        assert!(!line.contains("profile="), "no profile was named: {line}");

        // A profile records its name; the endpoint is the operator's.
        let named = kimmy_core::ProviderConfig::Profile { name: "corp".into() };
        let line = vectors_record_of("ConfigureVectors", Some(&named));
        assert!(line.contains("provider=profile"), "{line}");
        assert!(line.contains("profile=corp"), "{line}");
        assert!(!line.contains("endpoint_host="), "{line}");

        // A default endpoint is the default host, not an absence.
        let cohere = kimmy_core::ProviderConfig::Cohere {
            model: "embed-v4".into(),
            endpoint: None,
            api_key_env: "COHERE_API_KEY".into(),
        };
        let line = vectors_record_of("ConfigureVectors", Some(&cohere));
        assert!(line.contains("endpoint_host=api.cohere.com"), "{line}");

        // Disabling names no provider.
        let line = vectors_record_of("DisableVectors", None);
        assert!(line.contains("action=DisableVectors"), "{line}");
        assert!(!line.contains("provider="), "{line}");
    }

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
        assert!(is_write(Action::Ddl), "creating or dropping a collection changes state");
        assert!(is_write(Action::Admin));
    }
}
