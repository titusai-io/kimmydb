//! Peer discovery sources.
//!
//! KimmyDB has no seed list to hand-maintain and no leader to bootstrap from. A
//! node is told *where to look* for peers and re-resolves that periodically. In
//! Kubernetes this is the whole story: a headless Service resolves to every
//! ready pod IP, which is exactly the peer set — which is also why membership
//! gossip was not worth building ([ADR-037](../../../docs/decisions.md)).

use std::fmt;
use std::net::SocketAddr;

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
    /// which resolves to one A record per ready pod.
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
                let host = format!("{name}:{port}");
                match tokio::net::lookup_host(host.clone()).await {
                    Ok(addrs) => Ok(addrs.collect()),
                    Err(source) => Err(ResolveError::Lookup { target: host, source }),
                }
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
    #[error("could not resolve {target}: {source}")]
    Lookup { target: String, source: std::io::Error },
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

    fn parse(s: &str) -> SeedSource {
        s.parse().unwrap_or_else(|e| panic!("{s:?} should parse: {e}"))
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
        let source = parse("dns-srv:_kimmy._tcp.invalid.");
        let resolved = source.resolve().await;
        assert!(
            !matches!(&resolved, Err(ResolveError::Resolver(_))),
            "must reach a lookup rather than failing to build a resolver: {resolved:?}"
        );
    }
}
