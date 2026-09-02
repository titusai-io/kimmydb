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

use std::net::IpAddr;

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

/// What an operator has permitted beyond the public internet.
#[derive(Clone, Debug)]
pub struct EgressPolicy {
    purpose: Purpose,
    /// Hosts exempt from the address checks, matched case-insensitively on the
    /// URL's host. Empty means "public addresses only".
    allowed_hosts: Vec<String>,
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
    Blocked { host: String, addr: IpAddr },
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
        Self { purpose, allowed_hosts: allowed_hosts.iter().map(|h| h.to_lowercase()).collect() }
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

    /// Full check: shape, then every address the host resolves to.
    ///
    /// **Every** address, not the first: a name that resolves to one public and
    /// one private address would otherwise pass while still being usable to
    /// reach the private one.
    pub fn check(&self, url: &str) -> Result<(), EgressError> {
        let host = self.check_shape(url)?;
        if self.permits_host(host) {
            return Ok(());
        }
        // A literal address needs no resolver.
        if let Ok(addr) = host.parse::<IpAddr>() {
            return self.permits_addr(host, addr);
        }

        use std::net::ToSocketAddrs;
        let resolved: Vec<IpAddr> = (host, 80u16)
            .to_socket_addrs()
            .map_err(|_| self.refuse(Refusal::Unresolvable(host.to_string())))?
            .map(|sa| sa.ip())
            .collect();
        if resolved.is_empty() {
            return Err(self.refuse(Refusal::Unresolvable(host.to_string())));
        }
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
/// The up-front [`EgressPolicy::check`] stays. It is what refuses literal
/// addresses — which never reach a resolver — and it fails fast without
/// waiting for a connection attempt.
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
