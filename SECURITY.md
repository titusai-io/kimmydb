# Security policy

## Supported versions

KimmyDB is pre-1.0. A security fix ships as a patch release of the newest
`0.MINOR` line only; there are no backports to earlier minors. To receive a
fix, upgrade to the newest release. [docs/compatibility.md](docs/compatibility.md)
says what a minor bump may change, and the changelog says so when one does.

| Version | Supported |
|---|---|
| Newest `0.MINOR.x` | Yes |
| Anything older | No — upgrade |

## Reporting a vulnerability

Please do not open a public issue, and do not put the details in a pull
request.

Report it through GitHub's private vulnerability reporting: **Report a
vulnerability** under the repository's **Security** tab
(<https://github.com/titusai-io/kimmydb/security/advisories/new>). That opens a
private thread with the maintainers, and if the report leads to an advisory
it becomes one without the details having been public in between. If you
would rather not use GitHub, email <security@titusai.io>; ask for a key in
your first message if you want to encrypt.

What helps: the version (`kimmyd --version`, or the image tag), which
component, what an attacker can do with it, and steps to reproduce if you have
them.

What to expect:

- An acknowledgement within three business days.
- An assessment — confirmed, not reproducible, or out of scope, and why —
  within seven business days after that.
- A fix in a patch release of the current minor, with a changelog entry and a
  GitHub security advisory that credits you unless you would rather it did
  not. We ask for a reasonable window before public disclosure; ninety days
  from the report is the default, and sooner is fine once the fix has shipped.

## Scope

In scope: `kimmyd`, the container image at `ghcr.io/titusai-io/kimmydb`, the
release archives, and the workflows that build them. The `kimmy` CLI and the
client libraries moved to their own repositories and are not currently
distributed ([ADR-193](docs/decisions.md)). A vulnerability in a dependency counts when it is reachable from
one of these; tell us, and the release that closes it is ours to make.

Out of scope, because they are configuration rather than defects:

- A deployment that departs from what the [security guide](docs/security.md)
  and its deployment checklist say to do — a node run with
  `--insecure-no-auth` off loopback, a listener without TLS on a network you
  do not trust, `/metrics` left reachable from the internet, an example secret
  in production. The server refuses the configurations it can detect and the
  guide names the rest.
- Resource exhaustion by an authenticated caller beyond the limits the guide
  documents. The guide says plainly that denial of service is not defended
  against beyond login rate limiting.
- Findings from automated scanners with no demonstrated impact.

The guide's [What is NOT defended against](docs/security.md#what-is-not-defended-against)
lists the gaps we know about. A report that restates one of them is welcome
as a discussion, but it is not a vulnerability.

## What a release carries

Every release archive has a SHA-256 checksum beside it, and the release
workflow attests what it built — a signed build provenance statement for the
container image and the archives that `gh attestation verify` checks against
this repository. How to verify, and from which release the attestations
begin, is in [Verifying a release](docs/operations.md#verifying-a-release).
The dependency policy the graph is held to, and how updates arrive, is under
[Supply chain](docs/security.md#supply-chain).
