//! Which addresses the node may issue an outbound HTTP request to.
//!
//! # The hazard
//!
//! Two features make the *node* issue an outbound HTTP request to an address
//! a user supplied: a webhook, and an embedding provider named in a
//! collection's vector configuration. Without a policy either is a
//! server-side request forgery primitive: a principal who can register one can
//! make the database probe anything the node can reach — other services on the
//! private network, an admin port bound to loopback, and above all the cloud
//! metadata endpoint at `169.254.169.254`, which on most providers hands out
//! credentials to whoever asks from the instance.
//!
//! One policy for both, in one crate, because two copies of an address
//! denylist are two places for the next reserved range to be missing from.
//! The webhook and provider subsystems differ only in the wording of a
//! refusal — which noun, and which setting an operator adds a host to — and
//! that is what [`Purpose`] carries.
//!
//! # The policy
//!
//! Loopback, link-local, private and other non-public ranges are refused unless
//! an operator has explicitly allowed the host. Public addresses work with no
//! configuration, which is what keeps the features usable out of the box.
//!
//! # Checking the resolved address, not the name
//!
//! A hostname is not a destination. `evil.example.com` can resolve to a public
//! address when a webhook is registered and to `169.254.169.254` an hour later
//! — the classic DNS rebinding shape. So the name is resolved and **every**
//! address it resolves to is checked, at registration *and* again before each
//! request. Checking once, at registration, would validate a promise the DNS
//! can withdraw.
//!
//! Redirects are refused for the same reason: a permitted host that answers
//! `302 http://169.254.169.254/` would otherwise walk the request straight
//! through the policy. That part is the client's to enforce, and every client
//! built over [`CheckedResolver`] sets `redirect::Policy::none()`.
//!
//! # Resolving off the runtime's workers
//!
//! The system resolver is a blocking call: `getaddrinfo` waits out its own
//! timeouts and retries, and with search domains that can pass thirty seconds
//! for one name that does not answer. A check made from async code therefore
//! goes through [`EgressPolicy::check_async`], which hands the lookup to the
//! runtime's blocking pool and can be put under the caller's deadline. The
//! blocking [`EgressPolicy::check`] is for code that is allowed to block: the
//! configuration checks at startup and in `check-config`.
//!
//! A deadline abandons the wait for a lookup, not the lookup: `getaddrinfo`
//! cannot be cancelled, and holds its blocking thread until the resolver
//! answers. So the lookups are bounded, process-wide, against a resolver that
//! has stopped answering ([`MAX_LOOKUPS_IN_FLIGHT`], [`FAILED_LOOKUP_KEPT`]):
//!
//! - **One lookup per host at a time.** A check for a host already being
//!   looked up waits for that lookup's answer rather than starting another.
//! - **A failed lookup is remembered briefly.** For [`FAILED_LOOKUP_KEPT`]
//!   after a lookup fails, a check of that host fails at once with the same
//!   answer, so retries during an outage start no new lookups.
//! - **At most [`MAX_LOOKUPS_IN_FLIGHT`] at once** for the node's own requests
//!   (webhook deliveries, embedding provider calls), and **at most
//!   [`MAX_REQUEST_LOOKUPS_IN_FLIGHT`]** for checks made on a client's request
//!   (registering a webhook, configuring a provider), counted apart. Past its
//!   bound, a check fails at once as [`Refusal::LookupsBusy`], and its caller
//!   retries as it retries any lookup that failed. Apart, so that a client
//!   registering names that never answer fills only the request path's
//!   slots, and the deliveries and provider calls of everybody else go on.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// How many lookups [`EgressPolicy::check_async`] may have running at once,
/// across the process: a bound on the blocking threads a resolver that has
/// stopped answering can hold.
pub const MAX_LOOKUPS_IN_FLIGHT: usize = 16;

/// How many lookups [`EgressPolicy::check_for_request`] may have running at
/// once, across the process, apart from [`MAX_LOOKUPS_IN_FLIGHT`].
pub const MAX_REQUEST_LOOKUPS_IN_FLIGHT: usize = 4;

/// How long a failed lookup's answer is reused for its host.
pub const FAILED_LOOKUP_KEPT: Duration = Duration::from_secs(5);

/// What an egress policy guards, for the wording of a refusal.
///
/// The address rules are the same for every outbound request the node makes;
/// what differs is how a refusal reads. A person who pointed a webhook at a
/// private host needs to be told about `webhooks.allowed_hosts`, and a person
/// who pointed an embedding provider there about the provider setting — the
/// same message naming the wrong setting sends them to edit the wrong line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Purpose {
    /// The requests being policed, plural, as they read mid-sentence:
    /// `"webhooks"`, `"embedding providers"`.
    pub noun: &'static str,
    /// The setting an operator adds a host to.
    pub setting: &'static str,
}

impl Purpose {
    pub const fn new(noun: &'static str, setting: &'static str) -> Self {
        Self { noun, setting }
    }
}

/// A stand-in for the system resolver: see [`EgressPolicy::with_lookup`].
///
/// A blocking function, as the system resolver is, so that a stand-in which
/// hangs holds whatever thread calls it exactly as a lookup that does not
/// answer would.
pub type Lookup = Arc<dyn Fn(&str) -> std::io::Result<Vec<IpAddr>> + Send + Sync>;

/// What an operator has permitted beyond the public internet.
#[derive(Clone)]
pub struct EgressPolicy {
    purpose: Purpose,
    /// Hosts exempt from the address checks, matched case-insensitively on the
    /// URL's host. Empty means "public addresses only".
    allowed_hosts: Vec<String>,
    /// What resolves a host for [`Self::check`] and [`Self::check_async`], and
    /// the bounds on the lookups in flight. The system resolver's, shared by
    /// every policy in the process, unless a test set a stand-in with
    /// [`Self::with_lookup`]. The client's [`CheckedResolver`] does not use it.
    lookups: Arc<Lookups>,
}

impl std::fmt::Debug for EgressPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressPolicy")
            .field("purpose", &self.purpose)
            .field("allowed_hosts", &self.allowed_hosts)
            .field("lookup", &self.lookups.resolver.as_ref().map(|_| "injected"))
            .finish()
    }
}

/// A lookup's answer as the waiters share it: the addresses, or that there
/// were none.
type Answer = Result<Vec<IpAddr>, ()>;

/// The lookups [`EgressPolicy::check_async`] has started, with the bounds the
/// module's documentation gives.
struct Lookups {
    /// `None` is the system resolver.
    resolver: Option<Lookup>,
    pending: Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    /// The lookups running, by lowercased host, each on a blocking thread of
    /// its own: an entry leaves when its thread is done, whether or not
    /// anybody still waits for it.
    in_flight: HashMap<String, (Slots, tokio::sync::watch::Receiver<Option<Answer>>)>,
    /// When each host's last lookup failed.
    failed: HashMap<String, tokio::time::Instant>,
}

/// Which bound a lookup counts against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slots {
    /// The node's own requests: [`MAX_LOOKUPS_IN_FLIGHT`].
    Background,
    /// Checks made on a client's request: [`MAX_REQUEST_LOOKUPS_IN_FLIGHT`].
    Request,
}

impl Slots {
    fn limit(self) -> usize {
        match self {
            Slots::Background => MAX_LOOKUPS_IN_FLIGHT,
            Slots::Request => MAX_REQUEST_LOOKUPS_IN_FLIGHT,
        }
    }
}

/// Why a lookup gave no addresses.
enum Unanswered {
    /// The resolver found none, failed, or failed within
    /// [`FAILED_LOOKUP_KEPT`].
    Failed,
    /// The lookup's bound was full; it carries the bound.
    Busy(usize),
}

impl Lookups {
    fn new(resolver: Option<Lookup>) -> Arc<Self> {
        Arc::new(Self { resolver, pending: Mutex::new(Pending::default()) })
    }

    /// The process's one set, for the system resolver.
    fn system() -> Arc<Self> {
        static SYSTEM: OnceLock<Arc<Lookups>> = OnceLock::new();
        Arc::clone(SYSTEM.get_or_init(|| Self::new(None)))
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, Pending> {
        // Nothing under the lock can leave the maps half-written.
        self.pending.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Look a host up on the blocking pool, within the bounds. A check that
    /// joins a lookup already running takes no slot, whichever bound that
    /// lookup counts against.
    async fn resolve(
        self: &Arc<Self>,
        host: &str,
        slots: Slots,
    ) -> Result<Vec<IpAddr>, Unanswered> {
        let key = host.to_ascii_lowercase();
        let (mut answer, start) = {
            let mut pending = self.pending();
            let now = tokio::time::Instant::now();
            pending.failed.retain(|_, at| now.saturating_duration_since(*at) < FAILED_LOOKUP_KEPT);
            if pending.failed.contains_key(&key) {
                return Err(Unanswered::Failed);
            }
            match pending.in_flight.get(&key) {
                Some((_, running)) => (running.clone(), None),
                None => {
                    let taken = pending.in_flight.values().filter(|(s, _)| *s == slots).count();
                    if taken >= slots.limit() {
                        return Err(Unanswered::Busy(slots.limit()));
                    }
                    let (tell, answer) = tokio::sync::watch::channel(None);
                    pending.in_flight.insert(key.clone(), (slots, answer.clone()));
                    (answer, Some(tell))
                }
            }
        };
        // Started with the lock released: a runtime that is shutting down drops
        // the lookup on this thread, and `Done` takes the lock as it drops.
        if let Some(tell) = start {
            // Built here and moved into the lookup, so that the entry leaves
            // however the lookup ends: answered, panicking, or never run.
            let done = Done { lookups: Arc::clone(self), key, tell, answer: Err(()) };
            let resolver = self.resolver.clone();
            let host = host.to_string();
            // UNSUPERVISED: one blocking lookup; `Done` ends it in the
            // bookkeeping whatever happens to it, and its answer is waited for
            // below.
            drop(tokio::task::spawn_blocking(move || {
                let mut done = done;
                done.answer =
                    resolve(resolver.as_ref(), &host).ok().filter(|a| !a.is_empty()).ok_or(());
            }));
        }
        match answer.wait_for(Option::is_some).await {
            Ok(answered) => answered.clone().unwrap_or(Err(())).map_err(|()| Unanswered::Failed),
            Err(_) => Err(Unanswered::Failed),
        }
    }
}

/// A running lookup's end in the bookkeeping, on its thread when it drops.
struct Done {
    lookups: Arc<Lookups>,
    key: String,
    tell: tokio::sync::watch::Sender<Option<Answer>>,
    answer: Answer,
}

impl Drop for Done {
    fn drop(&mut self) {
        {
            let mut pending = self.lookups.pending();
            pending.in_flight.remove(&self.key);
            if self.answer.is_err() {
                pending.failed.insert(self.key.clone(), tokio::time::Instant::now());
            }
        }
        self.tell.send_replace(Some(self.answer.clone()));
    }
}

/// Why a URL was refused, and for which purpose.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EgressError {
    pub purpose: Purpose,
    pub refusal: Refusal,
}

/// The rule a URL failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    NotHttp(String),
    NoHost,
    Unresolvable(String),
    /// Not looked up: `limit` lookups, [`MAX_LOOKUPS_IN_FLIGHT`] or
    /// [`MAX_REQUEST_LOOKUPS_IN_FLIGHT`], were already waiting on the
    /// resolver.
    LookupsBusy {
        host: String,
        limit: usize,
    },
    Blocked {
        host: String,
        addr: IpAddr,
    },
}

impl EgressError {
    /// Whether the refusal is a condition of the moment rather than of the
    /// URL: a host that could not be resolved now, and is worth trying again.
    pub fn is_transient(&self) -> bool {
        matches!(self.refusal, Refusal::Unresolvable(_) | Refusal::LookupsBusy { .. })
    }

    /// Whether the host was not looked up at all, because its bound was full.
    /// Transient, and not the host's doing: a caller retries it without
    /// counting it against the host.
    pub fn is_lookups_busy(&self) -> bool {
        matches!(self.refusal, Refusal::LookupsBusy { .. })
    }
}

impl std::fmt::Display for EgressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Purpose { noun, setting } = self.purpose;
        match &self.refusal {
            Refusal::NotHttp(scheme) => {
                write!(f, "{noun} must use http or https URLs, got {scheme:?}")
            }
            Refusal::NoHost => write!(f, "{noun} need a URL with a host"),
            Refusal::Unresolvable(host) => {
                write!(f, "cannot resolve {host:?}")
            }
            Refusal::LookupsBusy { host, limit } => write!(
                f,
                "cannot resolve {host:?} now: {limit} lookups are already waiting on the resolver"
            ),
            Refusal::Blocked { host, addr } => write!(
                f,
                "{host:?} resolves to {addr}, which is not a public address, and {noun} may not \
                 reach loopback, link-local or private ranges — that would let one probe this \
                 node's own network and its cloud metadata endpoint. Add the host to {setting} \
                 if this is intended"
            ),
        }
    }
}

impl std::error::Error for EgressError {}

/// The host of an `http(s)` URL, with userinfo, port and path stripped.
///
/// `None` when the URL has no scheme separator or no host. Hand-rolled rather
/// than a URL crate because the shape needed is small and the one thing that
/// must not go wrong — reading the userinfo as the host — is easier to see in
/// ten lines than to trust to a parser's defaults.
pub fn host_of(url: &str) -> Option<&str> {
    let (_, rest) = url.split_once("://")?;
    // Authority ends at the first `/`, `?` or `#`.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // Strip userinfo and any port. An IPv6 literal is bracketed, so the
    // port separator is the colon *after* the closing bracket.
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let host = match authority.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    (!host.is_empty()).then_some(host)
}

impl EgressPolicy {
    pub fn new(purpose: Purpose, allowed_hosts: Vec<String>) -> Self {
        Self {
            purpose,
            allowed_hosts: allowed_hosts.iter().map(|h| h.to_lowercase()).collect(),
            lookups: Lookups::system(),
        }
    }

    /// The same policy, resolving hosts with `lookup` instead of the system
    /// resolver. See [`Lookup`]. The bounds are the stand-in's own, kept
    /// apart from the system resolver's and from any other stand-in's, and
    /// shared by the clones of the policy this returns.
    pub fn with_lookup(self, lookup: Lookup) -> Self {
        Self { lookups: Lookups::new(Some(lookup)), ..self }
    }

    /// Public addresses only, for the given purpose.
    pub fn public_only(purpose: Purpose) -> Self {
        Self::new(purpose, Vec::new())
    }

    pub fn purpose(&self) -> Purpose {
        self.purpose
    }

    fn refuse(&self, refusal: Refusal) -> EgressError {
        EgressError { purpose: self.purpose, refusal }
    }

    fn permits_host(&self, host: &str) -> bool {
        // Both sides are lowercased, because hostnames are case-insensitive and
        // a policy that was not would be bypassed by typing a capital letter.
        self.allowed_hosts.contains(&host.to_lowercase())
    }

    /// Check a URL's scheme and host shape, returning the host to resolve.
    ///
    /// Split from the address check so the cheap, network-free part can run
    /// before anything touches DNS — and so a test can exercise the address
    /// rules without a resolver.
    pub fn check_shape<'a>(&self, url: &'a str) -> Result<&'a str, EgressError> {
        let (scheme, _) = url.split_once("://").ok_or_else(|| self.refuse(Refusal::NoHost))?;
        let scheme = scheme.to_lowercase();
        if scheme != "http" && scheme != "https" {
            return Err(self.refuse(Refusal::NotHttp(scheme)));
        }
        host_of(url).ok_or_else(|| self.refuse(Refusal::NoHost))
    }

    /// Whether an address may be dialled.
    pub fn permits_addr(&self, host: &str, addr: IpAddr) -> Result<(), EgressError> {
        if self.permits_host(host) {
            return Ok(());
        }
        if is_public(addr) {
            return Ok(());
        }
        Err(self.refuse(Refusal::Blocked { host: host.to_string(), addr }))
    }

    /// Everything [`Self::check`] asks that needs no resolver: the URL's
    /// shape, the operator's allowlist, and a literal address.
    ///
    /// `Ok(None)` when that settles it, and `Ok(Some(host))` when the host is a
    /// name whose addresses still have to be checked. What a provider build
    /// asks, so that building one never waits on DNS.
    pub fn check_unresolved<'a>(&self, url: &'a str) -> Result<Option<&'a str>, EgressError> {
        let host = self.check_shape(url)?;
        if self.permits_host(host) {
            return Ok(None);
        }
        // A literal address needs no resolver.
        if let Ok(addr) = host.parse::<IpAddr>() {
            return self.permits_addr(host, addr).map(|()| None);
        }
        Ok(Some(host))
    }

    /// Full check: shape, then every address the host resolves to.
    ///
    /// **Every** address, not the first: a name that resolves to one public and
    /// one private address would otherwise pass while still being usable to
    /// reach the private one.
    ///
    /// **Blocks the calling thread** on the resolver for as long as it takes to
    /// answer, which no timeout can cut short. Never call it on a runtime
    /// worker: async code uses [`Self::check_async`].
    pub fn check(&self, url: &str) -> Result<(), EgressError> {
        let Some(host) = self.check_unresolved(url)? else {
            return Ok(());
        };
        let resolved = resolve(self.lookups.resolver.as_ref(), host)
            .map_err(|_| self.refuse(Refusal::Unresolvable(host.to_string())))?;
        self.permits_addrs(host, &resolved)
    }

    /// [`Self::check`], with the lookup on the runtime's blocking pool, within
    /// the bounds the module's documentation gives.
    ///
    /// The lookup cannot be cancelled, but the wait for it can: dropping this
    /// future (a deadline around it passing, the task being aborted at a stop)
    /// returns at once, and the lookup finishes on its blocking thread with
    /// nobody waiting for the answer. A caller puts it under the same deadline
    /// as the request it guards, so that a resolver that does not answer costs
    /// one failed request rather than a stalled worker.
    ///
    /// A host that could not be resolved is [`Refusal::Unresolvable`], whether
    /// its lookup failed now or within [`FAILED_LOOKUP_KEPT`]; one not looked
    /// up because [`MAX_LOOKUPS_IN_FLIGHT`] were running is
    /// [`Refusal::LookupsBusy`]. Both are conditions of the moment
    /// ([`EgressError::is_transient`]).
    ///
    /// For the node's own requests: a webhook delivery, an embedding provider
    /// call. A check made on a client's request is
    /// [`Self::check_for_request`].
    pub async fn check_async(&self, url: &str) -> Result<(), EgressError> {
        self.check_within(url, Slots::Background).await
    }

    /// [`Self::check_async`] for a check made on a client's request
    /// (registering a webhook, configuring a provider), counted against
    /// [`MAX_REQUEST_LOOKUPS_IN_FLIGHT`] rather than the node's own bound.
    pub async fn check_for_request(&self, url: &str) -> Result<(), EgressError> {
        self.check_within(url, Slots::Request).await
    }

    async fn check_within(&self, url: &str, slots: Slots) -> Result<(), EgressError> {
        let Some(host) = self.check_unresolved(url)? else {
            return Ok(());
        };
        let resolved =
            self.lookups.resolve(host, slots).await.map_err(|unanswered| match unanswered {
                Unanswered::Failed => self.refuse(Refusal::Unresolvable(host.to_string())),
                Unanswered::Busy(limit) => {
                    self.refuse(Refusal::LookupsBusy { host: host.to_string(), limit })
                }
            })?;
        self.permits_addrs(host, &resolved)
    }

    /// Check every address a host resolved to.
    ///
    /// Separate from [`Self::check`] so the rule can be tested without a
    /// resolver — there is no way to make DNS return a chosen pair of addresses
    /// from a unit test, and this is exactly the rule most worth pinning: a
    /// host answering with one public and one private address must be refused,
    /// not accepted on the strength of whichever happened to come first.
    pub fn permits_addrs(&self, host: &str, resolved: &[IpAddr]) -> Result<(), EgressError> {
        if resolved.is_empty() {
            return Err(self.refuse(Refusal::Unresolvable(host.to_string())));
        }
        for addr in resolved {
            self.permits_addr(host, *addr)?;
        }
        Ok(())
    }
}

/// Resolve a host's addresses, blocking, with the stand-in when one is set.
///
/// Port 80 is a placeholder: only the addresses matter.
fn resolve(lookup: Option<&Lookup>, host: &str) -> std::io::Result<Vec<IpAddr>> {
    match lookup {
        Some(lookup) => lookup(host),
        None => {
            use std::net::ToSocketAddrs;
            Ok((host, 80u16).to_socket_addrs()?.map(|sa| sa.ip()).collect())
        }
    }
}

/// A DNS resolver for an outbound client that checks what it resolves.
///
/// [`EgressPolicy::check`] resolves a hostname and checks every address — but
/// the connection is then made by the HTTP client, which resolves *again*, and
/// two resolutions are two answers. A name with a zero TTL can resolve
/// publicly for the check and inward for the dial, walking a blocked address
/// through the policy. Running the check inside the client's own resolver
/// closes that window: the addresses checked are, by construction, the
/// addresses dialled.
///
/// The up-front check ([`EgressPolicy::check_async`]) stays. It is what
/// refuses literal addresses, which never reach a resolver, and it fails fast
/// without waiting for a connection attempt. And behind a proxy it is the only
/// check of the target's addresses: a client that honours `HTTP_PROXY` or
/// `HTTPS_PROXY` resolves the proxy's name through this resolver, and never
/// the target's.
pub struct CheckedResolver {
    policy: EgressPolicy,
}

impl CheckedResolver {
    pub fn new(policy: EgressPolicy) -> Self {
        Self { policy }
    }
}

impl reqwest::dns::Resolve for CheckedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let policy = self.policy.clone();
        Box::pin(async move {
            let host = name.as_str().to_string();
            // Port 0 is a placeholder: the client replaces it with the URL's
            // port. Only the addresses matter here.
            let addrs: Vec<std::net::SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
            policy.permits_addrs(&host, &ips)?;
            Ok(Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = std::net::SocketAddr> + Send>)
        })
    }
}

/// Whether an address is on the public internet.
///
/// Written as a deny-list of the ranges that are *not* public, because the
/// standard library's `is_global` is unstable. Erring towards refusal: an
/// address this does not recognise as public is refused, so a range added to
/// the internet later is a false refusal rather than a hole.
fn is_public(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            !(v4.is_loopback()            // 127.0.0.0/8
                || v4.is_private()         // 10/8, 172.16/12, 192.168/16
                || v4.is_link_local()      // 169.254/16 — cloud metadata
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                || a == 0
                || a == 100 && (64..128).contains(&b) // 100.64/10 carrier NAT
                || a == 192 && b == 0                 // 192.0.0/24 protocol assignments
                || a == 198 && (18..20).contains(&b)  // 198.18/15 benchmarking
                || a >= 240) // 240/4 reserved
        }
        IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // fc00::/7 unique local, fe80::/10 link local
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                // An IPv4-mapped address is the IPv4 rules again, or a bypass.
                || v6.to_ipv4_mapped().is_some_and(|v4| !is_public(IpAddr::V4(v4))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WEBHOOKS: Purpose = Purpose::new("webhooks", "webhooks.allowed_hosts");

    fn open() -> EgressPolicy {
        EgressPolicy::public_only(WEBHOOKS)
    }

    fn allowing(hosts: &[&str]) -> EgressPolicy {
        EgressPolicy::new(WEBHOOKS, hosts.iter().map(|h| h.to_string()).collect())
    }

    #[test]
    fn public_addresses_are_allowed_with_no_configuration() {
        // The feature has to work out of the box, or every user's first
        // experience is a refusal.
        for addr in ["93.184.216.34", "1.1.1.1", "2606:4700:4700::1111"] {
            let addr: IpAddr = addr.parse().unwrap();
            open().permits_addr("example.com", addr).unwrap_or_else(|e| panic!("{addr}: {e}"));
        }
    }

    #[test]
    fn the_cloud_metadata_endpoint_is_refused() {
        // The single most valuable target: on most providers it hands out
        // credentials to anything that asks from the instance.
        let err = open().check("http://169.254.169.254/latest/meta-data/").unwrap_err();
        assert!(matches!(err.refusal, Refusal::Blocked { .. }), "{err:?}");
        assert!(err.to_string().contains("metadata"), "the error should say why: {err}");
    }

    #[test]
    fn a_refusal_names_the_callers_noun_and_setting() {
        // The one thing that differs between the subsystems sharing this
        // policy is which line an operator has to edit. A provider refusal
        // that told them about the webhook setting would send them to the
        // wrong one.
        let providers = EgressPolicy::public_only(Purpose::new(
            "embedding providers",
            "vector.provider.allowed_hosts",
        ));
        let err = providers.check("http://10.0.0.5/v1/embeddings").unwrap_err().to_string();
        assert!(err.contains("embedding providers may not reach"), "{err}");
        assert!(err.contains("vector.provider.allowed_hosts"), "{err}");
        assert!(!err.contains("webhook"), "{err}");

        let err = open().check("http://10.0.0.5/hook").unwrap_err().to_string();
        assert!(err.contains("webhooks may not reach"), "{err}");
        assert!(err.contains("webhooks.allowed_hosts"), "{err}");

        let err = providers.check("ftp://x/").unwrap_err().to_string();
        assert!(err.starts_with("embedding providers must use http or https"), "{err}");
    }

    #[test]
    fn loopback_and_private_ranges_are_refused() {
        for url in [
            "http://127.0.0.1:7878/",
            "http://localhost:7878/",
            "http://10.0.0.5/hook",
            "http://192.168.0.10/hook",
            "http://172.16.0.1/hook",
            "http://[::1]:7878/",
            "http://[fd00::1]/hook",
        ] {
            assert!(open().check(url).is_err(), "{url} should be refused");
        }
    }

    #[test]
    fn an_ipv4_mapped_ipv6_address_cannot_smuggle_a_private_target() {
        // ::ffff:127.0.0.1 is loopback wearing an IPv6 hat. Checking only the
        // IPv6 rules would wave it through.
        let addr: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(open().permits_addr("sneaky.example", addr).is_err());
        let addr: IpAddr = "::ffff:169.254.169.254".parse().unwrap();
        assert!(open().permits_addr("sneaky.example", addr).is_err());
    }

    #[test]
    fn one_private_address_among_public_ones_refuses_the_whole_host() {
        // A name can answer with several addresses, and an attacker controls
        // what theirs answers with. Checking only the first would let
        // `[93.184.216.34, 169.254.169.254]` through on the strength of the
        // address that was never going to be dialled.
        let policy = open();
        let public: IpAddr = "93.184.216.34".parse().unwrap();
        let metadata: IpAddr = "169.254.169.254".parse().unwrap();

        policy.permits_addrs("ok.example", &[public]).expect("all public is fine");

        for pair in [vec![public, metadata], vec![metadata, public]] {
            let err = policy
                .permits_addrs("mixed.example", &pair)
                .expect_err("a private address anywhere in the answer must refuse the host");
            assert!(matches!(err.refusal, Refusal::Blocked { .. }), "{err:?}");
        }
    }

    #[test]
    fn a_host_that_resolves_to_nothing_is_refused() {
        // Not silently allowed: an empty answer means the destination is
        // unknown, and unknown is not the same as safe.
        let err = open().permits_addrs("void.example", &[]).unwrap_err();
        assert!(matches!(err.refusal, Refusal::Unresolvable(_)), "{err:?}");
    }

    #[test]
    fn carrier_nat_and_reserved_ranges_are_refused() {
        for addr in ["100.64.0.1", "0.0.0.0", "240.0.0.1", "192.0.0.1"] {
            let addr: IpAddr = addr.parse().unwrap();
            assert!(open().permits_addr("h", addr).is_err(), "{addr} should be refused");
        }
    }

    #[test]
    fn an_operator_can_allow_a_specific_host() {
        // The escape hatch, for a webhook that genuinely targets something on
        // the private network.
        let policy = allowing(&["internal.corp"]);
        policy.check("http://internal.corp:9000/hook").expect("allowlisted host");
        // ...and only that host.
        assert!(policy.check("http://10.0.0.5/hook").is_err());
    }

    #[test]
    fn the_allowlist_is_case_insensitive() {
        // Hostnames are, so a policy that was not would be bypassable by
        // typing a capital letter.
        let policy = allowing(&["Internal.Corp"]);
        policy.check("http://INTERNAL.corp/hook").expect("case must not matter");
    }

    #[test]
    fn only_http_and_https_are_accepted() {
        for url in ["file:///etc/passwd", "gopher://x/", "ftp://x/"] {
            let err = open().check(url).unwrap_err();
            assert!(matches!(err.refusal, Refusal::NotHttp(_)), "{url}: {err:?}");
        }
    }

    #[test]
    fn the_host_is_parsed_out_of_the_awkward_shapes() {
        let p = open();
        assert_eq!(p.check_shape("https://example.com/a/b?c=1#d").unwrap(), "example.com");
        assert_eq!(p.check_shape("https://example.com:8443/").unwrap(), "example.com");
        assert_eq!(p.check_shape("https://[2001:db8::1]:8443/").unwrap(), "2001:db8::1");
        // Userinfo is where a naive parser reads the wrong host: everything
        // before `@` is credentials, and the destination is what follows.
        assert_eq!(p.check_shape("https://user:pass@169.254.169.254/").unwrap(), "169.254.169.254");
        assert!(p.check("https://user:pass@169.254.169.254/").is_err());
        // The bare host reader agrees, and says nothing about the scheme.
        assert_eq!(host_of("https://user:pass@169.254.169.254/"), Some("169.254.169.254"));
        assert_eq!(host_of("notaurl"), None);
        assert_eq!(host_of("https:///path"), None);
    }

    #[test]
    fn a_url_with_no_host_is_refused() {
        for url in ["https://", "notaurl", "https:///path"] {
            assert!(open().check(url).is_err(), "{url}");
        }
    }

    /// A lookup answering with the given addresses, counting its calls.
    fn answering(addrs: &[&str]) -> (Lookup, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let addrs: Vec<IpAddr> = addrs.iter().map(|a| a.parse().unwrap()).collect();
        let lookup: Lookup = Arc::new(move |_host: &str| {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if addrs.is_empty() {
                Err(std::io::Error::other("no such host"))
            } else {
                Ok(addrs.clone())
            }
        });
        (lookup, calls)
    }

    /// The async check asks the same questions as the blocking one, of the
    /// addresses the lookup gives: every address checked, an unanswered name
    /// unresolvable, and a literal or allowlisted host never looked up.
    #[tokio::test]
    async fn the_async_check_applies_the_rules_to_what_the_lookup_answers() {
        let (public, _) = answering(&["93.184.216.34"]);
        open().with_lookup(public).check_async("https://ok.example/hook").await.unwrap();

        let (mixed, _) = answering(&["93.184.216.34", "169.254.169.254"]);
        let err =
            open().with_lookup(mixed).check_async("https://mixed.example/").await.unwrap_err();
        assert!(matches!(err.refusal, Refusal::Blocked { .. }), "{err:?}");

        let (nothing, _) = answering(&[]);
        let err =
            open().with_lookup(nothing).check_async("https://void.example/").await.unwrap_err();
        assert_eq!(err.refusal, Refusal::Unresolvable("void.example".into()));
        assert_eq!(err.to_string(), "cannot resolve \"void.example\"", "the message is unchanged");

        // Settled without a lookup: a literal address, and an allowlisted name.
        let (counted, calls) = answering(&["93.184.216.34"]);
        let policy = allowing(&["internal.corp"]).with_lookup(counted);
        let err = policy.check_async("http://10.0.0.5/hook").await.unwrap_err();
        assert!(matches!(err.refusal, Refusal::Blocked { .. }), "{err:?}");
        policy.check_async("http://internal.corp/hook").await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        // And the blocking check uses the same stand-in.
        policy.check("http://public.example/hook").unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// A stand-in that holds every caller until released, counting calls: a
    /// resolver that has stopped answering. Released at the latest after
    /// [`HUNG_FOR`], so that a test of broken code fails rather than hangs.
    struct Gate {
        released: Mutex<bool>,
        wake: std::sync::Condvar,
        calls: std::sync::atomic::AtomicUsize,
    }

    const HUNG_FOR: Duration = Duration::from_secs(20);

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                released: Mutex::new(false),
                wake: std::sync::Condvar::new(),
                calls: Default::default(),
            })
        }

        fn lookup(self: &Arc<Self>) -> Lookup {
            let gate = Arc::clone(self);
            Arc::new(move |_host: &str| {
                gate.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let released = gate.released.lock().unwrap();
                let _ = gate.wake.wait_timeout_while(released, HUNG_FOR, |r| !*r).unwrap();
                Err(std::io::Error::other("the gate was released"))
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.wake.notify_all();
        }

        /// Yield until the stand-in has been called `times` times. The
        /// ceiling, in real time, is for calls that never come, so that such a
        /// test fails rather than spins: it times nothing that passes.
        async fn until_called(&self, times: usize) {
            let started = std::time::Instant::now();
            while self.calls() < times {
                assert!(started.elapsed() < HUNG_FOR, "the lookups were never called");
                tokio::task::yield_now().await;
            }
        }
    }

    /// **Checks of one host share one lookup.** Eight deliveries to a host
    /// whose resolver does not answer used to be eight blocked threads; they
    /// are one, and every check gets its answer. The host is matched without
    /// regard to case.
    #[tokio::test]
    async fn concurrent_checks_of_one_host_start_one_lookup() {
        let gate = Gate::new();
        let policy = open().with_lookup(gate.lookup());
        let checks: Vec<_> = (0..8)
            .map(|i| {
                let policy = policy.clone();
                let url = if i == 0 { "http://HANGS.example/" } else { "http://hangs.example/" };
                tokio::spawn(async move { policy.check_async(url).await })
            })
            .collect();
        gate.until_called(1).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(checks.iter().all(|c| !c.is_finished()), "every check waits for the answer");
        gate.release();
        for check in checks {
            let err = check.await.unwrap().unwrap_err();
            assert!(matches!(err.refusal, Refusal::Unresolvable(_)), "{err:?}");
        }
        assert_eq!(gate.calls(), 1, "one lookup for all eight");
    }

    /// **A host is one lookup, whatever case it is written in.** Names are
    /// case-insensitive, so checks of `Example.TEST` and `example.test` made
    /// together join one lookup, and a failed one is reused for both.
    #[tokio::test]
    async fn a_host_in_mixed_case_is_one_lookup() {
        let gate = Gate::new();
        let policy = open().with_lookup(gate.lookup());
        let checks: Vec<_> =
            ["http://Example.TEST/", "http://example.test/", "http://EXAMPLE.test/"]
                .into_iter()
                .map(|url| {
                    let policy = policy.clone();
                    tokio::spawn(async move { policy.check_async(url).await })
                })
                .collect();
        gate.until_called(1).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gate.calls(), 1, "one lookup while all three wait");
        gate.release();
        for check in checks {
            let err = check.await.unwrap().unwrap_err();
            assert!(matches!(err.refusal, Refusal::Unresolvable(_)), "{err:?}");
        }
        policy.check_async("http://eXaMpLe.test/").await.unwrap_err();
        assert_eq!(gate.calls(), 1, "and the failure is kept under the same key");
    }

    /// **A failed lookup is not repeated within [`FAILED_LOOKUP_KEPT`].** A
    /// retry during an outage gets the same answer at once and starts no new
    /// lookup; another host is looked up as usual, and the failed one again
    /// once the window has passed. Time is paused and moved by hand.
    #[tokio::test(start_paused = true)]
    async fn a_failed_lookup_is_not_repeated_within_the_window() {
        let (failing, calls) = answering(&[]);
        let policy = open().with_lookup(failing);
        let calls = || calls.load(std::sync::atomic::Ordering::SeqCst);
        for _ in 0..3 {
            let err = policy.check_async("http://void.example/").await.unwrap_err();
            assert_eq!(err.refusal, Refusal::Unresolvable("void.example".into()));
        }
        assert_eq!(calls(), 1, "the answer is reused within the window");

        policy.check_async("http://other.example/").await.unwrap_err();
        assert_eq!(calls(), 2, "another host is looked up");

        tokio::time::advance(FAILED_LOOKUP_KEPT).await;
        policy.check_async("http://void.example/").await.unwrap_err();
        assert_eq!(calls(), 3, "and the failed host again once the window has passed");
    }

    /// **At most [`MAX_LOOKUPS_IN_FLIGHT`] lookups run at once.** Sixteen hosts
    /// whose lookups do not answer, each waited for under a deadline that
    /// passes: the waits are abandoned and the lookups are not, so they still
    /// count. A seventeenth host is refused at once, as a condition of the
    /// moment, with no lookup started. A host already being looked up is
    /// joined rather than refused, and the bound has room again once the
    /// lookups end.
    #[tokio::test(start_paused = true)]
    async fn lookups_past_the_bound_are_refused_at_once_as_transient() {
        let gate = Gate::new();
        let policy = open().with_lookup(gate.lookup());
        let waits: Vec<_> = (0..MAX_LOOKUPS_IN_FLIGHT)
            .map(|i| {
                let policy = policy.clone();
                tokio::spawn(async move {
                    let url = format!("http://h{i}.example/");
                    tokio::time::timeout(Duration::from_secs(1), policy.check_async(&url)).await
                })
            })
            .collect();
        gate.until_called(MAX_LOOKUPS_IN_FLIGHT).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        for wait in waits {
            assert!(wait.await.unwrap().is_err(), "the wait was abandoned at its deadline");
        }

        let err = policy.check_async("http://h16.example/").await.unwrap_err();
        assert_eq!(
            err.refusal,
            Refusal::LookupsBusy { host: "h16.example".into(), limit: MAX_LOOKUPS_IN_FLIGHT }
        );
        assert!(err.is_transient(), "{err:?}");
        assert!(err.to_string().contains("16 lookups are already waiting"), "{err}");
        assert_eq!(gate.calls(), MAX_LOOKUPS_IN_FLIGHT, "no lookup was started for it");

        let joined = tokio::spawn({
            let policy = policy.clone();
            async move { policy.check_async("http://h3.example/").await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        gate.release();
        let err = joined.await.unwrap().unwrap_err();
        assert_eq!(err.refusal, Refusal::Unresolvable("h3.example".into()), "joined, not refused");

        // The lookups end on their own threads; then the bound has room.
        let err = loop {
            match policy.check_async("http://h16.example/").await {
                Err(EgressError { refusal: Refusal::LookupsBusy { .. }, .. }) => {
                    tokio::task::yield_now().await
                }
                other => break other.unwrap_err(),
            }
        };
        assert_eq!(err.refusal, Refusal::Unresolvable("h16.example".into()));
        assert_eq!(gate.calls(), MAX_LOOKUPS_IN_FLIGHT + 1);
    }

    /// **A client's request cannot take the node's own lookups.** Checks made
    /// on a request (registering a webhook, configuring a provider) count
    /// against a bound of their own, so a client registering names that never
    /// answer fills those slots only: a delivery's or a provider call's check
    /// still starts its lookup, up to the node's own bound.
    #[tokio::test(start_paused = true)]
    async fn a_requests_lookups_and_the_nodes_own_are_bounded_apart() {
        let gate = Gate::new();
        let policy = open().with_lookup(gate.lookup());
        let spawn_checks = |prefix: &'static str, n: usize, request: bool| -> Vec<_> {
            (0..n)
                .map(|i| {
                    let policy = policy.clone();
                    tokio::spawn(async move {
                        let url = format!("http://{prefix}{i}.example/");
                        if request {
                            policy.check_for_request(&url).await
                        } else {
                            policy.check_async(&url).await
                        }
                    })
                })
                .collect()
        };

        let requests = spawn_checks("r", MAX_REQUEST_LOOKUPS_IN_FLIGHT, true);
        gate.until_called(MAX_REQUEST_LOOKUPS_IN_FLIGHT).await;
        let err = policy.check_for_request("http://r-more.example/").await.unwrap_err();
        assert_eq!(
            err.refusal,
            Refusal::LookupsBusy {
                host: "r-more.example".into(),
                limit: MAX_REQUEST_LOOKUPS_IN_FLIGHT
            }
        );
        assert!(err.to_string().contains("4 lookups are already waiting"), "{err}");

        let node = spawn_checks("n", MAX_LOOKUPS_IN_FLIGHT, false);
        gate.until_called(MAX_REQUEST_LOOKUPS_IN_FLIGHT + MAX_LOOKUPS_IN_FLIGHT).await;
        let err = policy.check_async("http://n-more.example/").await.unwrap_err();
        assert!(err.is_lookups_busy(), "{err:?}");
        assert_eq!(gate.calls(), MAX_REQUEST_LOOKUPS_IN_FLIGHT + MAX_LOOKUPS_IN_FLIGHT);

        gate.release();
        for check in requests.into_iter().chain(node) {
            let err = check.await.unwrap().unwrap_err();
            assert!(matches!(err.refusal, Refusal::Unresolvable(_)), "{err:?}");
        }
    }

    /// **A successful answer is never reused.** Only a failure is kept: a name
    /// that answered publicly and then inward is looked up again, and refused,
    /// on the second check.
    #[tokio::test]
    async fn a_successful_lookup_is_never_reused() {
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = Arc::clone(&n);
        let lookup: Lookup = Arc::new(move |_h: &str| {
            let i = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![if i == 0 { "93.184.216.34" } else { "10.0.0.5" }.parse().unwrap()])
        });
        let policy = open().with_lookup(lookup);
        policy.check_async("http://rebind.example/").await.unwrap();
        let err = policy.check_async("http://rebind.example/").await.unwrap_err();
        assert!(matches!(err.refusal, Refusal::Blocked { .. }), "{err:?}");
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// **Policies that share lookups never share a verdict.** A lookup's answer
    /// is addresses, and each policy judges them against its own allowlist:
    /// one that lists the host passes while one that does not is refused, in
    /// either order and whichever started the lookup. (The allowlisted
    /// policy never reaches the lookup for a host it lists, so it is the
    /// other policy's checks that are counted.)
    #[tokio::test]
    async fn policies_sharing_lookups_never_share_a_verdict() {
        let (lookup, calls) = answering(&["10.0.0.5"]);
        let public = open().with_lookup(lookup);
        let allowing_internal =
            EgressPolicy { allowed_hosts: vec!["internal.example".into()], ..public.clone() };
        for _ in 0..2 {
            let (a, b) = tokio::join!(
                allowing_internal.check_async("http://internal.example/"),
                public.check_async("http://internal.example/")
            );
            a.unwrap();
            assert!(matches!(b.unwrap_err().refusal, Refusal::Blocked { .. }));
            let (b, a) = tokio::join!(
                public.check_async("http://internal.example/"),
                allowing_internal.check_async("http://internal.example/")
            );
            a.unwrap();
            assert!(matches!(b.unwrap_err().refusal, Refusal::Blocked { .. }));
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    /// **A lookup that panics leaves no count and no waiter behind.** Twice the
    /// bound of panicking lookups, one after another: each waiter gets an
    /// unresolvable answer, none is refused for the bound, and nothing is left
    /// in flight.
    #[tokio::test]
    async fn a_panicking_lookup_leaves_no_count_behind() {
        let lookup: Lookup = Arc::new(|_h: &str| panic!("the stand-in resolver panicked"));
        let policy = open().with_lookup(lookup);
        for i in 0..(MAX_LOOKUPS_IN_FLIGHT * 2) {
            let url = format!("http://p{i}.example/");
            let err = tokio::time::timeout(Duration::from_secs(20), policy.check_async(&url))
                .await
                .expect("a waiter hung on a panicked lookup")
                .unwrap_err();
            assert_eq!(err.refusal, Refusal::Unresolvable(format!("p{i}.example")));
        }
        assert!(policy.lookups.pending().in_flight.is_empty());
    }

    #[test]
    fn the_unresolved_check_settles_what_it_can_and_names_the_host_otherwise() {
        let policy = allowing(&["internal.corp"]);
        assert_eq!(policy.check_unresolved("http://internal.corp/").unwrap(), None);
        assert_eq!(policy.check_unresolved("http://93.184.216.34/").unwrap(), None);
        assert_eq!(
            policy.check_unresolved("http://example.com:8080/x").unwrap(),
            Some("example.com")
        );
        let err = policy.check_unresolved("http://169.254.169.254/").unwrap_err();
        assert!(matches!(err.refusal, Refusal::Blocked { .. }), "{err:?}");
        let err = policy.check_unresolved("ftp://example.com/").unwrap_err();
        assert!(matches!(err.refusal, Refusal::NotHttp(_)), "{err:?}");
    }

    #[tokio::test]
    async fn the_client_refuses_a_blocked_answer_at_dial_time() {
        // The TOCTOU this resolver exists to close: `check` resolving one
        // answer and the client dialling another. `localhost` resolves to
        // loopback locally, with no external DNS involved, so it stands in for
        // the name that "resolves inward" — and the refusal must come from the
        // resolver inside the client, because nothing else here checks it.
        let client = reqwest::Client::builder()
            .dns_resolver(std::sync::Arc::new(CheckedResolver::new(open())))
            .build()
            .unwrap();
        let err = client.get("http://localhost:9/").send().await.unwrap_err();
        // The chain debug-formats the source, so the refusal appears as the
        // `Blocked` variant rather than its Display text.
        let chain = format!("{err:?}");
        assert!(chain.contains("Blocked"), "expected the egress refusal: {chain}");
    }

    #[tokio::test]
    async fn the_client_resolver_honours_the_allowlist() {
        // The operator's escape hatch has to survive the move into the
        // resolver, or allowlisting a private host would pass registration and
        // then fail every delivery. Port 1 is expected to refuse the
        // connection — what matters is that the failure is a socket error, not
        // the policy.
        let client = reqwest::Client::builder()
            .dns_resolver(std::sync::Arc::new(CheckedResolver::new(allowing(&["localhost"]))))
            .build()
            .unwrap();
        let err = client.get("http://localhost:1/").send().await.unwrap_err();
        let chain = format!("{err:?}");
        assert!(!chain.contains("Blocked"), "the allowlist was ignored: {chain}");
    }
}
