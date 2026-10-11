//! Peer discovery sources.
//!
//! KimmyDB has no seed list to hand-maintain and no leader to bootstrap from. A
//! node is told *where to look* for peers and re-resolves that periodically. In
//! Kubernetes this is the whole story: a headless Service resolves to every
//! pod IP, which is exactly the peer set — which is also why membership
//! gossip was not worth building ([ADR-037](../../../docs/decisions.md)). Every
//! pod, ready or not, when the Service sets `publishNotReadyAddresses`, as the
//! operations guide's does: otherwise only ready pods can be found, and a
//! cluster starting from cold has none.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use hickory_resolver::proto::rr::RData;
use serde::{Deserialize, Serialize};

/// Where to look for peers.
///
/// Parsed from a compact string form so it can come from a CLI flag, an
/// environment variable, or a TOML file without three different shapes.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum SeedSource {
    /// `static:10.0.0.1:7900,10.0.0.2:7900` — explicit addresses.
    Static(Vec<SocketAddr>),
    /// `dns:seeds.kimmy.internal` — A/AAAA records, each paired with `port`.
    Dns { name: String, port: u16 },
    /// `dns-srv:_kimmy._tcp.example.com` — SRV records carry their own ports,
    /// so peers need not agree on one in advance.
    DnsSrv { name: String },
    /// `k8s:kimmy-headless.default.svc.cluster.local` — a headless Service,
    /// which resolves to one A record per pod (per ready pod, unless it sets
    /// `publishNotReadyAddresses`).
    Kubernetes { name: String, port: u16 },
}

/// Replication port used when a discovery form does not carry one of its own.
pub const DEFAULT_CLUSTER_PORT: u16 = 7900;

impl SeedSource {
    /// Resolve this source to the addresses it currently names.
    ///
    /// Called periodically rather than once: a Kubernetes headless Service
    /// gains and loses records as pods come and go, and a node that resolved
    /// only at startup would never see a peer that joined after it.
    ///
    /// Failure to resolve is *not* an error worth propagating — a DNS name that
    /// is temporarily unresolvable is the normal state of a cluster starting
    /// up, and treating it as fatal would make the first node to boot refuse to
    /// run. Callers get an empty list and try again next tick.
    pub async fn resolve(&self) -> Result<Vec<SocketAddr>, ResolveError> {
        match self {
            Self::Static(addrs) => Ok(addrs.clone()),
            Self::Dns { name, port } | Self::Kubernetes { name, port } => {
                resolve_host(name, *port, SEED_LOOKUP_BOUND).await
            }
            // SRV names a host *and* a port, which the standard library cannot
            // read — it resolves names, not arbitrary record types.
            Self::DnsSrv { name } => resolve_srv(name).await,
        }
    }

    /// A human-readable description for startup logs.
    pub fn describe(&self) -> String {
        match self {
            Self::Static(addrs) => format!("{} static peer(s)", addrs.len()),
            Self::Dns { name, port } => format!("DNS A/AAAA {name} on port {port}"),
            Self::DnsSrv { name } => format!("DNS SRV {name}"),
            Self::Kubernetes { name, port } => {
                format!("Kubernetes headless service {name} on port {port}")
            }
        }
    }
}

/// Whether `seeds` name a member other than the one at `own`, judged without
/// resolving anything (ADR-202).
///
/// A member that creates its store with nobody else to hear from has nothing to
/// catch up from, so it is not marked. A static address is compared with this
/// member's own cluster address, which is how discovery already takes it out of
/// what a seed resolves to (`resolve`). A name (`dns:`, `dns-srv:`, `k8s:`) is
/// resolved by the discovery loop and not at the start, so it is taken to name
/// others: the member is marked, and waits the bound if nobody answers.
pub fn names_another_member(seeds: &[SeedSource], own: SocketAddr) -> bool {
    seeds.iter().any(|seed| match seed {
        SeedSource::Static(addrs) => addrs.iter().any(|addr| *addr != own),
        SeedSource::Dns { .. } | SeedSource::DnsSrv { .. } | SeedSource::Kubernetes { .. } => true,
    })
}

/// How long one `dns:` or `k8s:` seed may take to resolve before it is given up
/// for this tick.
///
/// A lookup is `timeout` × `attempts` per name in the search list, one name after
/// another, so a resolver that drops queries can keep one going for minutes.
/// Half the default discovery interval: longer than a single dead nameserver
/// takes to give up on one name (5 s × 2), short enough that the next tick is
/// not still waiting on this one. The loop starts no second resolution while one
/// is in flight (`peers::replicate`), so a bound is what lets a tick fail at all.
pub const SEED_LOOKUP_BOUND: Duration = Duration::from_secs(15);

/// `KIMMY_TEST_DNS_SERVER=ip:port`, as `kimmyd` read it: resolve `dns:` and
/// `k8s:` seeds against this one name server and nothing from the host's
/// `resolv.conf`.
///
/// `resolv.conf` cannot name a port, so a test of a real `kimmyd` could not
/// otherwise point it at a server of its own. Set at the start and never
/// outside a test.
static TEST_DNS_SERVER: std::sync::RwLock<Option<SocketAddr>> = std::sync::RwLock::new(None);

/// The tests that set the process-wide test name server hold this one at a
/// time: it is global, and the tests of this crate run in one process.
#[cfg(test)]
pub(crate) static TEST_DNS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Resolve `dns:` and `k8s:` seeds against `server` alone (a test switch).
pub fn set_test_dns_server(server: Option<SocketAddr>) {
    if let Ok(mut set) = TEST_DNS_SERVER.write() {
        *set = server;
    }
}

/// A resolver for one lookup, built from the host's DNS configuration as it is
/// now.
///
/// **Built per lookup, and dropped with it.** The library reads `/etc/hosts` and
/// `/etc/resolv.conf` when a resolver is built and keeps them, and keeps an LRU
/// of answers that honours record TTLs, negative ones included. `getaddrinfo`
/// does none of that: it reads both files on every call and keeps nothing. A
/// shared resolver would never see an edit to either file, and would answer the
/// discovery loop's early retries (250 ms, 500 ms, …) from a cached "no such
/// name" for as long as the Service's negative TTL (30 s on CoreDNS), which is
/// exactly when a cluster starting from cold is looking for its first peer.
fn fresh_resolver() -> Result<hickory_resolver::TokioResolver, ResolveError> {
    let test_server = TEST_DNS_SERVER.read().ok().and_then(|server| *server);
    let built = match test_server {
        Some(server) => {
            let mut ns = hickory_resolver::config::NameServerConfig::udp(server.ip());
            for connection in &mut ns.connections {
                connection.port = server.port();
            }
            let config =
                hickory_resolver::config::ResolverConfig::from_parts(None, Vec::new(), vec![ns]);
            hickory_resolver::TokioResolver::builder_with_config(
                config,
                hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
            )
            .build()
        }
        None => hickory_resolver::TokioResolver::builder_tokio()
            .map_err(|e| ResolveError::Resolver(e.to_string()))?
            .build(),
    };
    built.map_err(|e| ResolveError::Resolver(e.to_string()))
}

/// Resolve `name` to every address it holds, each paired with `port`.
async fn resolve_host(
    name: &str,
    port: u16,
    bound: Duration,
) -> Result<Vec<SocketAddr>, ResolveError> {
    resolve_host_with(fresh_resolver, name, port, bound).await
}

/// [`resolve_host`], with the resolver built by `build`, so the tests can point
/// each lookup at a DNS server of their own.
///
/// Dropping this future ends the lookup: no thread is blocked on it, which is
/// what `getaddrinfo` on the blocking pool could not offer, and why a stop no
/// longer waits for one.
async fn resolve_host_with(
    build: impl FnOnce() -> Result<hickory_resolver::TokioResolver, ResolveError>,
    name: &str,
    port: u16,
    bound: Duration,
) -> Result<Vec<SocketAddr>, ResolveError> {
    let target = format!("{name}:{port}");
    let resolver = build()?;
    match tokio::time::timeout(bound, resolver.lookup_ip(name)).await {
        Ok(Ok(ips)) => Ok(ips.iter().map(|ip| SocketAddr::new(ip, port)).collect()),
        Ok(Err(e)) => Err(ResolveError::Lookup { target, detail: e.to_string() }),
        Err(_) => Err(ResolveError::TimedOut { target, after: bound }),
    }
}

/// The process-wide resolver, built from `/etc/resolv.conf` on first use.
///
/// One resolver rather than one per lookup: it holds the system configuration
/// and a response cache, and rebuilding it every discovery tick would re-read
/// that file and throw the cache away each time.
static RESOLVER: std::sync::OnceLock<hickory_resolver::TokioResolver> = std::sync::OnceLock::new();

/// The resolver, building it if this is the first call.
///
/// A failure is **not** cached. Reading the system configuration can fail
/// transiently — a container whose `/etc/resolv.conf` is still being written —
/// and a poisoned cell would turn one bad moment at startup into a node that
/// can never discover anything.
fn resolver() -> Result<&'static hickory_resolver::TokioResolver, ResolveError> {
    if let Some(resolver) = RESOLVER.get() {
        return Ok(resolver);
    }
    let built = hickory_resolver::TokioResolver::builder_tokio()
        .map_err(|e| ResolveError::Resolver(e.to_string()))?
        .build()
        .map_err(|e| ResolveError::Resolver(e.to_string()))?;
    Ok(RESOLVER.get_or_init(|| built))
}

/// Resolve an SRV name to the addresses its records point at.
///
/// Two steps, because an SRV record names a **host and a port**, not an
/// address: read the SRV records, then resolve each target and pair every
/// address it yields with the port that named it. That pairing is the whole
/// reason SRV exists here — it is what lets peers run on ports nothing has to
/// agree on in advance.
///
/// A target that will not resolve is skipped rather than failing the set. Its
/// peer is one of several, the others are still reachable, and a cluster where
/// one pod is mid-restart is an ordinary state rather than an error.
async fn resolve_srv(name: &str) -> Result<Vec<SocketAddr>, ResolveError> {
    resolve_srv_with(resolver()?, name).await
}

/// The two-step resolution, against a resolver the caller supplies.
///
/// Split out so the tests can point it at a DNS server of their own and drive
/// the real code path, rather than asserting against the internet.
async fn resolve_srv_with(
    resolver: &hickory_resolver::TokioResolver,
    name: &str,
) -> Result<Vec<SocketAddr>, ResolveError> {
    let lookup = match resolver.srv_lookup(name).await {
        Ok(lookup) => lookup,
        // "The name exists, and has no SRV records" is what a cluster looks
        // like before its first node registers, so it is an empty set rather
        // than a failure. The caller warns on every error it gets, once per
        // discovery tick — reporting this one would mean a warning every tick
        // forever while nothing is actually wrong.
        Err(e) if e.is_no_records_found() => return Ok(Vec::new()),
        Err(e) => {
            return Err(ResolveError::Srv { name: name.to_string(), detail: e.to_string() });
        }
    };

    let mut addrs = Vec::new();
    for record in lookup.answers() {
        // The answer section can legitimately hold records that are not SRV —
        // a CNAME in the chain, most commonly — so this filters rather than
        // treating anything else as a malformed reply.
        let RData::SRV(srv) = &record.data else {
            continue;
        };
        let target = srv.target.to_utf8();
        match resolver.lookup_ip(target.clone()).await {
            Ok(ips) => addrs.extend(ips.iter().map(|ip| SocketAddr::new(ip, srv.port))),
            Err(e) => {
                tracing::debug!(
                    target = %target,
                    error = %e,
                    "an SRV target did not resolve; skipping it and keeping the rest"
                );
            }
        }
    }

    // Two SRV records may point at one host, and a target with both an A and a
    // AAAA record yields two addresses for one peer. Duplicates would be
    // harmless but would inflate every "discovered N peers" line.
    addrs.sort();
    addrs.dedup();
    Ok(addrs)
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("could not resolve {target}: {detail}")]
    Lookup { target: String, detail: String },
    #[error("could not resolve {target}: no answer in {after:?}")]
    TimedOut { target: String, after: Duration },
    #[error("could not look up SRV records for {name}: {detail}")]
    Srv { name: String, detail: String },
    #[error("could not build a DNS resolver from the system configuration: {0}")]
    Resolver(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ParseSeedError {
    #[error(
        "unknown discovery scheme {0:?}: expected one of static:, dns:, dns-srv:, k8s: \
         (bare host:port is also accepted)"
    )]
    UnknownScheme(String),
    #[error("invalid socket address {0:?}")]
    BadAddress(String),
    #[error("invalid port in {0:?}")]
    BadPort(String),
    #[error("discovery target must not be empty")]
    Empty,
}

impl std::str::FromStr for SeedSource {
    type Err = ParseSeedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseSeedError::Empty);
        }

        let Some((scheme, rest)) = s.split_once(':') else {
            return Err(ParseSeedError::UnknownScheme(s.to_string()));
        };
        let rest = rest.trim();
        if rest.is_empty() {
            return Err(ParseSeedError::Empty);
        }

        // A bare `host:port` is the most likely thing someone types by reflex,
        // so accept it as static rather than making them learn a scheme.
        let looks_like_bare_addr =
            !matches!(scheme, "static" | "dns" | "dns-srv" | "srv" | "k8s" | "kubernetes");
        if looks_like_bare_addr {
            return parse_static(s);
        }

        match scheme {
            "static" => parse_static(rest),
            "dns" => {
                let (name, port) = split_host_port(rest)?;
                Ok(Self::Dns { name, port })
            }
            "dns-srv" | "srv" => Ok(Self::DnsSrv { name: rest.to_string() }),
            "k8s" | "kubernetes" => {
                let (name, port) = split_host_port(rest)?;
                Ok(Self::Kubernetes { name, port })
            }
            other => Err(ParseSeedError::UnknownScheme(other.to_string())),
        }
    }
}

fn parse_static(list: &str) -> Result<SeedSource, ParseSeedError> {
    let addrs = list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<SocketAddr>().map_err(|_| ParseSeedError::BadAddress(s.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    if addrs.is_empty() {
        return Err(ParseSeedError::Empty);
    }
    Ok(SeedSource::Static(addrs))
}

/// Split an optional `:port` suffix off a DNS name, defaulting the port.
///
/// IPv6 literals are not valid DNS names, so a bare colon is unambiguous here.
fn split_host_port(s: &str) -> Result<(String, u16), ParseSeedError> {
    match s.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => {
            let port = port.parse().map_err(|_| ParseSeedError::BadPort(s.to_string()))?;
            Ok((host.to_string(), port))
        }
        _ => Ok((s.to_string(), DEFAULT_CLUSTER_PORT)),
    }
}

impl fmt::Display for SeedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(addrs) => {
                let joined = addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(",");
                write!(f, "static:{joined}")
            }
            Self::Dns { name, port } => write!(f, "dns:{name}:{port}"),
            Self::DnsSrv { name } => write!(f, "dns-srv:{name}"),
            Self::Kubernetes { name, port } => write!(f, "k8s:{name}:{port}"),
        }
    }
}

impl From<SeedSource> for String {
    fn from(s: SeedSource) -> Self {
        s.to_string()
    }
}

impl TryFrom<String> for SeedSource {
    type Error = ParseSeedError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    fn parse(s: &str) -> SeedSource {
        s.parse().unwrap_or_else(|e| panic!("{s:?} should parse: {e}"))
    }

    /// Seeds that name only this member name nobody to hear from; anything that
    /// names another address, or a name discovery resolves later, does.
    #[test]
    fn seeds_naming_only_this_member_name_no_other() {
        let own: SocketAddr = "127.0.0.1:7900".parse().unwrap();
        let other: SocketAddr = "127.0.0.2:7900".parse().unwrap();
        let same_ip_other_port: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        assert!(!names_another_member(&[], own));
        assert!(!names_another_member(&[SeedSource::Static(vec![])], own));
        assert!(!names_another_member(&[SeedSource::Static(vec![own])], own));
        assert!(!names_another_member(
            &[SeedSource::Static(vec![own]), SeedSource::Static(vec![own])],
            own
        ));
        assert!(names_another_member(&[SeedSource::Static(vec![own, other])], own));
        assert!(names_another_member(&[SeedSource::Static(vec![other])], own));
        assert!(names_another_member(&[SeedSource::Static(vec![same_ip_other_port])], own));
        for name in
            ["dns:seeds.example.com", "dns-srv:_kimmy._tcp.example.com", "k8s:kimmy.default.svc"]
        {
            assert!(names_another_member(&[parse(name)], own), "{name}");
        }
        assert!(names_another_member(
            &[SeedSource::Static(vec![own]), parse("dns:x.example.com")],
            own
        ));
    }

    #[test]
    fn parses_each_scheme() {
        assert_eq!(
            parse("static:10.0.0.1:7900,10.0.0.2:7900"),
            SeedSource::Static(vec![
                "10.0.0.1:7900".parse().unwrap(),
                "10.0.0.2:7900".parse().unwrap(),
            ])
        );
        assert_eq!(
            parse("dns:seeds.kimmy.internal"),
            SeedSource::Dns { name: "seeds.kimmy.internal".into(), port: DEFAULT_CLUSTER_PORT }
        );
        assert_eq!(
            parse("dns:seeds.kimmy.internal:9999"),
            SeedSource::Dns { name: "seeds.kimmy.internal".into(), port: 9999 }
        );
        assert_eq!(
            parse("dns-srv:_kimmy._udp.example.com"),
            SeedSource::DnsSrv { name: "_kimmy._udp.example.com".into() }
        );
        assert_eq!(
            parse("k8s:kimmy-headless.default.svc.cluster.local"),
            SeedSource::Kubernetes {
                name: "kimmy-headless.default.svc.cluster.local".into(),
                port: DEFAULT_CLUSTER_PORT
            }
        );
    }

    #[test]
    fn accepts_a_bare_host_port() {
        // Nobody should have to read the docs to point at one known peer.
        assert_eq!(
            parse("192.168.0.5:7900"),
            SeedSource::Static(vec!["192.168.0.5:7900".parse().unwrap()])
        );
    }

    #[test]
    fn rejects_nonsense() {
        assert!("".parse::<SeedSource>().is_err());
        assert!("dns:".parse::<SeedSource>().is_err());
        assert!("static:not-an-address".parse::<SeedSource>().is_err());
        assert!("dns:host:notaport".parse::<SeedSource>().is_err());
    }

    #[test]
    fn display_round_trips_through_parse() {
        for s in [
            "static:10.0.0.1:7900",
            "dns:seeds.internal:7900",
            "dns-srv:_kimmy._udp.example.com",
            "k8s:svc.default.svc.cluster.local:7900",
        ] {
            let parsed = parse(s);
            assert_eq!(parse(&parsed.to_string()), parsed, "round trip failed for {s}");
        }
    }

    #[test]
    fn serde_uses_the_compact_string_form() {
        let source = parse("k8s:kimmy-headless.default.svc.cluster.local");
        let json = serde_json::to_string(&source).unwrap();
        assert!(json.starts_with('"'), "expected a bare string, got {json}");
        assert_eq!(serde_json::from_str::<SeedSource>(&json).unwrap(), source);
    }

    // -----------------------------------------------------------------------
    // SRV resolution
    //
    // Driven against a DNS server this test runs, not against the internet: a
    // test that needs a real zone is a test that fails on an aeroplane, and
    // one that needs the internet cannot assert what the answer should be.
    // -----------------------------------------------------------------------

    use std::net::{IpAddr, Ipv4Addr};

    use hickory_resolver::config::{NameServerConfig, ResolverConfig};
    use hickory_resolver::proto::op::{Message, ResponseCode};
    use hickory_resolver::proto::rr::rdata::{A, SRV};
    use hickory_resolver::proto::rr::{Name, Record, RecordType};

    /// One SRV target: the name it points at, its port, and the addresses that
    /// name resolves to.
    struct Target {
        host: &'static str,
        port: u16,
        addrs: Vec<Ipv4Addr>,
    }

    /// A UDP DNS server that answers SRV and A queries for a fixed zone.
    ///
    /// Small enough to read, which is the point: a mock that lies about the
    /// wire format would make the test pass without the code being right.
    async fn dns_server(srv_name: &'static str, targets: Vec<Target>) -> SocketAddr {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                    return;
                };
                let Ok(request) = Message::from_vec(&buf[..len]) else {
                    continue;
                };
                let Some(query) = request.queries.first().cloned() else {
                    continue;
                };

                let mut response = Message::response(request.id, request.op_code);
                response.add_query(query.clone());
                response.metadata.response_code = ResponseCode::NoError;
                response.metadata.authoritative = true;
                // A real client checks these bits, so the fake server has to
                // set them or the answer is discarded before it is read.
                response.metadata.recursion_desired = request.metadata.recursion_desired;
                response.metadata.recursion_available = true;

                let asked = query.name().to_utf8();
                match query.query_type() {
                    RecordType::SRV if asked.trim_end_matches('.') == srv_name => {
                        for t in &targets {
                            let target = Name::from_utf8(t.host).unwrap();
                            response.add_answer(Record::from_rdata(
                                query.name().clone(),
                                60,
                                RData::SRV(SRV::new(0, 0, t.port, target)),
                            ));
                        }
                    }
                    RecordType::A => {
                        for t in &targets {
                            if asked.trim_end_matches('.') != t.host.trim_end_matches('.') {
                                continue;
                            }
                            for ip in &t.addrs {
                                response.add_answer(Record::from_rdata(
                                    query.name().clone(),
                                    60,
                                    RData::A(A(*ip)),
                                ));
                            }
                        }
                    }
                    // AAAA and anything else: an empty NOERROR, which is what a
                    // real server says for a name that exists without that
                    // record type.
                    _ => {}
                }

                if let Ok(bytes) = response.to_vec() {
                    let _ = socket.send_to(&bytes, from).await;
                }
            }
        });
        addr
    }

    /// A resolver that talks only to the given server.
    fn resolver_pointed_at(server: SocketAddr) -> hickory_resolver::TokioResolver {
        let mut ns = NameServerConfig::udp(server.ip());
        // The test server listens on an ephemeral port, so the port has to be
        // set rather than left at 53.
        for connection in &mut ns.connections {
            connection.port = server.port();
        }
        let config = ResolverConfig::from_parts(None, Vec::new(), vec![ns]);
        let mut builder = hickory_resolver::TokioResolver::builder_with_config(
            config,
            hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
        );
        builder.options_mut().attempts = 1;
        builder.build().unwrap()
    }

    #[tokio::test]
    async fn srv_records_resolve_to_their_targets_paired_with_their_own_ports() {
        // The property that makes SRV worth having: each peer's port comes off
        // its own record, so two peers on different ports both work without
        // anything agreeing on a port in advance.
        let server = dns_server(
            "_kimmy._tcp.example.com",
            vec![
                Target {
                    host: "a.example.com",
                    port: 7900,
                    addrs: vec![Ipv4Addr::new(10, 0, 0, 1)],
                },
                Target {
                    host: "b.example.com",
                    port: 7901,
                    addrs: vec![Ipv4Addr::new(10, 0, 0, 2)],
                },
            ],
        )
        .await;
        let resolver = resolver_pointed_at(server);

        let addrs = resolve_srv_with(&resolver, "_kimmy._tcp.example.com").await.unwrap();

        assert_eq!(
            addrs,
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7900),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7901),
            ],
            "each address must carry the port from the SRV record that named its host"
        );
    }

    #[tokio::test]
    async fn a_target_with_several_addresses_yields_one_entry_each() {
        let server = dns_server(
            "_kimmy._tcp.example.com",
            vec![Target {
                host: "multi.example.com",
                port: 7900,
                addrs: vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)],
            }],
        )
        .await;

        let addrs = resolve_srv_with(&resolver_pointed_at(server), "_kimmy._tcp.example.com")
            .await
            .unwrap();

        assert_eq!(addrs.len(), 2, "both A records must become peers: {addrs:?}");
        assert!(addrs.iter().all(|a| a.port() == 7900), "both carry the record's port");
    }

    #[tokio::test]
    async fn a_target_that_does_not_resolve_is_skipped_rather_than_failing_the_set() {
        // One pod mid-restart must not cost a node every other peer it was
        // told about.
        let server = dns_server(
            "_kimmy._tcp.example.com",
            vec![
                Target {
                    host: "up.example.com",
                    port: 7900,
                    addrs: vec![Ipv4Addr::new(10, 0, 0, 1)],
                },
                // Named by an SRV record, but with no address records.
                Target { host: "down.example.com", port: 7901, addrs: vec![] },
            ],
        )
        .await;

        let addrs = resolve_srv_with(&resolver_pointed_at(server), "_kimmy._tcp.example.com")
            .await
            .unwrap();

        assert_eq!(
            addrs,
            vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7900)],
            "the reachable peer survives its unreachable neighbour"
        );
    }

    #[tokio::test]
    async fn a_name_with_no_srv_records_resolves_to_nothing_rather_than_erroring() {
        // An empty answer is the normal state of a cluster whose first node has
        // not registered yet. Treating it as an error would make the first node
        // to boot log a failure every discovery tick.
        let server = dns_server("_kimmy._tcp.example.com", vec![]).await;

        let addrs = resolve_srv_with(&resolver_pointed_at(server), "_kimmy._tcp.example.com").await;

        assert!(matches!(addrs.as_deref(), Ok([])), "expected an empty set, got {addrs:?}");
    }

    #[tokio::test]
    async fn a_resolver_that_cannot_answer_is_an_error_not_an_empty_set() {
        // The other side of the no-records rule, and the one that matters more.
        // "No SRV records yet" is an empty set on purpose; a resolver that
        // cannot be reached at all must **not** take the same path, or a node
        // whose DNS is broken would look exactly like a cluster waiting for its
        // first member — silently, every tick, forever.
        let dead = "127.0.0.1:1".parse::<SocketAddr>().unwrap();
        let resolved =
            resolve_srv_with(&resolver_pointed_at(dead), "_kimmy._tcp.example.com").await;

        assert!(
            matches!(resolved, Err(ResolveError::Srv { .. })),
            "an unreachable resolver must be reported, got {resolved:?}"
        );
    }

    #[tokio::test]
    async fn the_seed_source_reaches_srv_resolution_at_all() {
        // Guards the wiring rather than the resolution: `dns-srv:` returned a
        // "not implemented" error for four milestones, and the parse tests
        // above passed the whole time.
        //
        // Alone among the tests here it goes through the process-wide
        // resolver, which is built from the host's own DNS configuration, so
        // how long a guaranteed-nonexistent name takes to fail is the host's
        // business and not this crate's: an immediate NXDOMAIN where the
        // resolver is a local stub, minutes of retries where it is not. That
        // is why the lookup is bounded and **a timeout is a pass** — the
        // failure this test exists to catch, a resolver that cannot be built
        // at all, returns before the lookup starts.
        let source = parse("dns-srv:_kimmy._tcp.invalid.");
        let Ok(resolved) = timeout(Duration::from_secs(2), source.resolve()).await else {
            return;
        };
        assert!(
            !matches!(&resolved, Err(ResolveError::Resolver(_))),
            "must reach a lookup rather than failing to build a resolver: {resolved:?}"
        );
    }

    // -----------------------------------------------------------------------
    // dns: and k8s: resolution (H1)
    // -----------------------------------------------------------------------

    use hickory_resolver::proto::rr::rdata::{AAAA, SOA};
    use std::collections::BTreeMap;
    use std::net::Ipv6Addr;
    use std::sync::Mutex;

    /// What a [`zone_server`] knows and what it was asked.
    #[derive(Default)]
    struct Zone {
        /// Names (no trailing dot) and the addresses they hold.
        hosts: Mutex<BTreeMap<String, Vec<IpAddr>>>,
        /// Every name asked for, in order, A and AAAA alike, as the server read it.
        asked: Mutex<Vec<String>>,
        /// The negative answer's TTL, in a SOA the server attaches to it: what a
        /// real server says for a name it does not have.
        negative_ttl: u32,
    }

    impl Zone {
        fn with(hosts: &[(&str, &[IpAddr])]) -> Arc<Zone> {
            let zone = Zone { negative_ttl: 30, ..Zone::default() };
            for (name, addrs) in hosts {
                zone.hosts.lock().unwrap().insert((*name).into(), addrs.to_vec());
            }
            Arc::new(zone)
        }

        fn add(&self, name: &str, addrs: &[IpAddr]) {
            self.hosts.lock().unwrap().insert(name.into(), addrs.to_vec());
        }

        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }
    }

    use std::sync::Arc;

    /// A UDP DNS server over `zone`: A and AAAA for the names it holds, and for
    /// any other name NXDOMAIN with a SOA, as a real server answers.
    async fn zone_server(zone: Arc<Zone>) -> SocketAddr {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                let Ok((len, from)) = socket.recv_from(&mut buf).await else {
                    return;
                };
                let Ok(request) = Message::from_vec(&buf[..len]) else {
                    continue;
                };
                let Some(query) = request.queries.first().cloned() else {
                    continue;
                };
                let asked = query.name().to_utf8().trim_end_matches('.').to_ascii_lowercase();
                zone.asked.lock().unwrap().push(asked.clone());

                let mut response = Message::response(request.id, request.op_code);
                response.add_query(query.clone());
                response.metadata.authoritative = true;
                response.metadata.recursion_desired = request.metadata.recursion_desired;
                response.metadata.recursion_available = true;

                let held = zone.hosts.lock().unwrap().get(&asked).cloned();
                match held {
                    Some(addrs) => {
                        response.metadata.response_code = ResponseCode::NoError;
                        for ip in addrs {
                            let rdata = match (query.query_type(), ip) {
                                (RecordType::A, IpAddr::V4(ip)) => RData::A(A(ip)),
                                (RecordType::AAAA, IpAddr::V6(ip)) => RData::AAAA(AAAA(ip)),
                                _ => continue,
                            };
                            response.add_answer(Record::from_rdata(
                                query.name().clone(),
                                60,
                                rdata,
                            ));
                        }
                    }
                    None => {
                        response.metadata.response_code = ResponseCode::NXDomain;
                        let apex = Name::from_utf8("test.").unwrap();
                        response.add_authority(Record::from_rdata(
                            apex.clone(),
                            zone.negative_ttl,
                            RData::SOA(SOA::new(
                                apex.clone(),
                                apex,
                                1,
                                3600,
                                600,
                                86400,
                                zone.negative_ttl,
                            )),
                        ));
                    }
                }
                if let Ok(bytes) = response.to_vec() {
                    let _ = socket.send_to(&bytes, from).await;
                }
            }
        });
        addr
    }

    /// A name server that reads every query and never answers one.
    async fn silent_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            while socket.recv_from(&mut buf).await.is_ok() {}
        });
        (addr, task)
    }

    /// A resolver builder for `server`, with `search` as the host's search list.
    fn builder_for(
        server: SocketAddr,
        search: &'static [&'static str],
    ) -> impl Fn() -> Result<hickory_resolver::TokioResolver, ResolveError> {
        move || {
            let mut ns = NameServerConfig::udp(server.ip());
            for connection in &mut ns.connections {
                connection.port = server.port();
            }
            let search = search.iter().map(|d| Name::from_utf8(d).unwrap()).collect();
            let config = ResolverConfig::from_parts(None, search, vec![ns]);
            let mut builder = hickory_resolver::TokioResolver::builder_with_config(
                config,
                hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
            );
            builder.options_mut().attempts = 1;
            builder.build().map_err(|e| ResolveError::Resolver(e.to_string()))
        }
    }

    const V4: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    const V4_B: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    const V6: IpAddr = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1));

    #[tokio::test]
    async fn a_host_resolves_to_every_address_it_holds_each_with_the_seeds_port() {
        let zone = Zone::with(&[("seeds.test", &[V4, V4_B, V6])]);
        let server = zone_server(zone).await;

        let mut found =
            resolve_host_with(builder_for(server, &[]), "seeds.test", 7911, Duration::from_secs(5))
                .await
                .unwrap();
        found.sort();

        let mut want: Vec<SocketAddr> = [V4, V4_B, V6].map(|ip| SocketAddr::new(ip, 7911)).into();
        want.sort();
        assert_eq!(found, want, "both families, every address, the seed's port");
    }

    #[tokio::test]
    async fn a_name_that_does_not_exist_is_an_error_and_not_an_empty_set() {
        // Unlike `dns-srv:`, where "no records yet" is the normal state: a
        // mistyped `dns:` name has to stay visible, as it was under getaddrinfo.
        let server = zone_server(Zone::with(&[])).await;

        let err = resolve_host_with(
            builder_for(server, &[]),
            "nobody.test",
            7900,
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ResolveError::Lookup { .. }), "{err:?}");
        assert!(err.to_string().contains("nobody.test:7900"), "names the seed: {err}");
    }

    #[tokio::test]
    async fn a_lookup_nobody_answers_gives_up_at_its_bound() {
        let (server, _task) = silent_server().await;
        let bound = Duration::from_millis(300);

        let started = std::time::Instant::now();
        let err = resolve_host_with(builder_for(server, &[]), "hung.test", 7900, bound)
            .await
            .unwrap_err();

        assert!(matches!(err, ResolveError::TimedOut { after, .. } if after == bound), "{err:?}");
        let took = started.elapsed();
        assert!(took >= bound && took < bound + Duration::from_secs(2), "{took:?}");
    }

    #[test]
    fn dropping_a_lookup_nobody_answers_ends_it() {
        // What a stop does to a resolution: the loop's guard aborts the task. The
        // future goes with it, and the runtime that held it ends at once, where a
        // blocking-pool `getaddrinfo` held the runtime until it returned.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (server, _task) = runtime.block_on(silent_server());
        let lookup = runtime.spawn(resolve_host_with(
            builder_for(server, &[]),
            "hung.test",
            7900,
            Duration::from_secs(60),
        ));
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(200)).await });
        lookup.abort();

        let started = std::time::Instant::now();
        runtime.shutdown_timeout(Duration::from_secs(30));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the runtime ended with the lookup: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_name_is_looked_up_afresh_each_time_and_a_cached_no_is_not_kept() {
        // The discovery loop asks again after 250 ms, 500 ms, ... while a cluster
        // starts, and a Service answers "no such name" with a 30 s TTL until its
        // first pod registers. A resolver that kept that answer would turn every
        // early retry into the same "no".
        let zone = Zone::with(&[]);
        let server = zone_server(zone.clone()).await;
        let build = builder_for(server, &[]);

        // The control, so the test has power: one resolver, asked twice, keeps
        // its first answer.
        let shared = build().unwrap();
        assert!(shared.lookup_ip("late.test").await.is_err());
        zone.add("late.test", &[V4]);
        assert!(
            shared.lookup_ip("late.test").await.is_err(),
            "the control: a resolver kept across ticks answers from its cache"
        );

        // The code under test builds one per lookup: asked before the name
        // exists, then after, as the early retries are.
        zone.hosts.lock().unwrap().clear();
        let before = resolve_host_with(&build, "late.test", 7900, Duration::from_secs(5)).await;
        assert!(before.is_err(), "the name does not exist yet: {before:?}");
        zone.add("late.test", &[V4]);
        let found = resolve_host_with(&build, "late.test", 7900, Duration::from_secs(5))
            .await
            .expect("the name appeared, and the next lookup sees it");
        assert_eq!(found, vec![SocketAddr::new(V4, 7900)]);
    }

    #[tokio::test]
    async fn a_short_name_is_tried_under_the_search_list_and_a_final_dot_is_not() {
        let zone = Zone::with(&[("db.corp.test", &[V4])]);
        let server = zone_server(zone.clone()).await;

        let found = resolve_host_with(
            builder_for(server, &["corp.test."]),
            "db",
            7900,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(found, vec![SocketAddr::new(V4, 7900)], "found under the search domain");

        let before = zone.asked().len();
        let absolute = resolve_host_with(
            builder_for(server, &["corp.test."]),
            "db.corp.test.",
            7900,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_eq!(absolute, found);
        let asked = &zone.asked()[before..];
        assert!(
            asked.iter().all(|name| name == "db.corp.test"),
            "a name with a final dot is asked for alone: {asked:?}"
        );
    }

    #[tokio::test]
    async fn a_numeric_host_is_returned_as_it_is() {
        let (server, _task) = silent_server().await;
        let found =
            resolve_host_with(builder_for(server, &[]), "10.1.2.3", 7900, Duration::from_secs(5))
                .await
                .unwrap();
        assert_eq!(found, vec!["10.1.2.3:7900".parse::<SocketAddr>().unwrap()]);
    }

    #[tokio::test]
    async fn the_test_switch_points_the_seed_source_at_one_name_server() {
        let _only = super::TEST_DNS_LOCK.lock().await;
        let zone = Zone::with(&[("seeds.test", &[V4])]);
        set_test_dns_server(Some(zone_server(zone.clone()).await));

        let dns = parse("dns:seeds.test").resolve().await;
        let k8s = parse("k8s:seeds.test").resolve().await;
        let missing = parse("dns:missing.test").resolve().await;
        set_test_dns_server(None);

        assert_eq!(dns.unwrap(), vec![SocketAddr::new(V4, DEFAULT_CLUSTER_PORT)]);
        assert_eq!(k8s.unwrap(), vec![SocketAddr::new(V4, DEFAULT_CLUSTER_PORT)]);
        assert!(missing.is_err());
        assert!(!zone.asked().is_empty(), "the lookups went to the switch's server");
    }
}
