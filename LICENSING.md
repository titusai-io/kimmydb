# Licensing

KimmyDB is developed and maintained by Titus AI LLC. This document explains,
in plain language, which license applies to which part of the repository and
what that means for you. The license texts themselves are authoritative; this
is a guide, not a substitute.

## The short version

| Component | License | In practice |
|---|---|---|
| The server — `kimmyd` and the crates it is built from (`kimmy-core`, `kimmy-storage`, `kimmy-query`, `kimmy-vector`, `kimmy-auth`, `kimmy-cluster`, `kimmy-api`, `kimmy-mcp`, `kimmy-task`) | [GNU Affero General Public License v3.0](LICENSE) | Free to run, self-host, and modify — personally or at a business. If you modify it and let others use it over a network, you must make your modified source available under the same license. |
| Documentation and the conformance suite | Apache License 2.0 | Same as the clients. |
| Commercial license | Contact <licensing@titusai.io> | For anyone who wants to embed, redistribute, or build on the server without the AGPL's obligations. |

## What the AGPL means for you

You can:

- Run KimmyDB for anything, including in production at a company, for free.
- Read, modify, and build the source.
- Redistribute it, modified or not, under the AGPL.

You must, if you **distribute** a modified server or **offer a modified server
to others over a network**:

- Make the complete corresponding source of your modifications available under
  the AGPL.

You do **not** trigger any obligation by:

- Running an unmodified `kimmyd` (from a release, the container image, or a
  build of this repository) behind your own application. Your application is
  not a derivative of the database it talks to.
- Using the client libraries. They are Apache-2.0 precisely so that this
  question never comes up for application authors.

The line is the same one MongoDB (pre-2018), MySQL, and Grafana drew: the
database engine is copyleft, the drivers are permissive, and talking to the
server over its protocol makes nothing a derivative work.

## When you need a commercial license

- You embed KimmyDB inside a product you ship to customers and do not want to
  release that product's source.
- You offer a modified KimmyDB as a service and do not want to publish the
  modifications.
- Your organization's policy excludes AGPL software regardless of how it is
  used.
- You want support, indemnification, or an SLA.

Write to <licensing@titusai.io>.

## Contributing

Contributions are welcome. Because the server is dual-licensed (AGPL and
commercial), every contributor is asked to sign the
[Contributor License Agreement](CLA.md) once, before their first pull request is
merged. The CLA grants Titus AI the rights needed to keep offering both
licenses; you retain copyright in your work.

## Third-party software

KimmyDB depends on open-source crates and modules under their own licenses.
`cargo license` (Rust), `go-licenses` (Go), and `pip-licenses` (Python) report
the full set for each component.
