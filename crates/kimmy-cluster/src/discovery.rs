//! Peer discovery sources.
//!
//! KimmyDB has no seed list to hand-maintain and no leader to bootstrap from. A
//! node is told *where to look* for peers, re-resolves that periodically, and
//! feeds whatever it finds to the SWIM layer. In Kubernetes this is the whole
//! story: a headless Service resolves to every ready pod IP, which is exactly
//! the seed set.

use std::fmt;
use std::net::SocketAddr;

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
    /// `dns-srv:_kimmy._udp.example.com` — SRV records carry their own ports.
    DnsSrv { name: String },
    /// `k8s:kimmy-headless.default.svc.cluster.local` — a headless Service,
    /// which resolves to one A record per ready pod.
    Kubernetes { name: String, port: u16 },
}

/// Gossip port used when a discovery form does not carry one of its own.
pub const DEFAULT_GOSSIP_PORT: u16 = 7900;

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
            // SRV records carry their own ports, which the standard library
            // cannot read — it resolves names, not arbitrary record types. A
            // resolver crate would be needed; see docs/deviations.md.
            Self::DnsSrv { name } => Err(ResolveError::SrvUnsupported(name.clone())),
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

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("could not resolve {target}: {source}")]
    Lookup { target: String, source: std::io::Error },
    #[error(
        "SRV discovery ({0}) needs a DNS resolver that can read SRV records, which is not \
         built yet; use dns: or k8s: with an explicit port"
    )]
    SrvUnsupported(String),
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
        _ => Ok((s.to_string(), DEFAULT_GOSSIP_PORT)),
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
            SeedSource::Dns { name: "seeds.kimmy.internal".into(), port: DEFAULT_GOSSIP_PORT }
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
                port: DEFAULT_GOSSIP_PORT
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
}
